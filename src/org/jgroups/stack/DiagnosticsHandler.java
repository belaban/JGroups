package org.jgroups.stack;

import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Bela Ban
 * @since 3.0
 */

public class DiagnosticsHandler implements Runnable {
    public static final String        THREAD_NAME = "DiagnosticsHandler";
    protected Thread                  thread=null;
    protected MulticastSocket         diag_sock=null;
    protected InetAddress             diagnostics_addr=null;
    protected int                     diagnostics_port=7500;
    protected int                     ttl=8;
    protected List<NetworkInterface>  bind_interfaces=null;
    protected final Set<ProbeHandler> handlers=new CopyOnWriteArraySet<ProbeHandler>();
    protected final Log               log;
    protected final SocketFactory     socket_factory;
    protected final ThreadFactory     thread_factory;
    protected final String    passcode;


    public DiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port,
             Log log, SocketFactory socket_factory, ThreadFactory thread_factory) {
       this(diagnostics_addr,diagnostics_port,log,socket_factory,thread_factory,null);
    }
    
    public DiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port,
                              Log log, SocketFactory socket_factory, ThreadFactory thread_factory, String passcode) {
        this.diagnostics_addr=diagnostics_addr;
        this.diagnostics_port=diagnostics_port;
        this.log=log;
        this.socket_factory=socket_factory;
        this.thread_factory=thread_factory;
        this.passcode = passcode;
    }

    public DiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port,
                              List<NetworkInterface> bind_interfaces, int diagnostics_ttl,
                              Log log, SocketFactory socket_factory, ThreadFactory thread_factory, String passcode) {
        this(diagnostics_addr, diagnostics_port, log, socket_factory, thread_factory, passcode);
        this.bind_interfaces=bind_interfaces;
        this.ttl=diagnostics_ttl;
    }

    public Thread getThread(){
        return thread;
    }

    public Set<ProbeHandler> getProbeHandlers() {return handlers;}

    public void registerProbeHandler(ProbeHandler handler) {
        if(handler != null)
            handlers.add(handler);
    }

    public void unregisterProbeHandler(ProbeHandler handler) {
        if(handler != null)
            handlers.remove(handler);
    }

    public void start() throws IOException {
        // https://jira.jboss.org/jira/browse/JGRP-777 - this doesn't work on MacOS, and we don't have
        // cross talking on Windows anyway, so we just do it for Linux. (How about Solaris ?)
        // if(can_bind_to_mcast_addr)
        //   diag_sock=Util.createMulticastSocket(getSocketFactory(),
        //                               Global.TP_DIAG_MCAST_SOCK, diagnostics_addr, diagnostics_port, log);
        //else
        diag_sock=socket_factory.createMulticastSocket("jgroups.tp.diag.mcast_sock", diagnostics_port);
        diag_sock.setTimeToLive(ttl);

        List<NetworkInterface> interfaces=bind_interfaces != null? bind_interfaces : Util.getAllAvailableInterfaces();
        bindToInterfaces(interfaces, diag_sock);

        if(thread == null || !thread.isAlive()) {
            thread=thread_factory.newThread(this, THREAD_NAME);
            thread.setDaemon(true);
            thread.start();
        }
    }

    public void stop() {
        if(diag_sock != null)
            socket_factory.close(diag_sock);
        handlers.clear();
        if(thread != null){
            try{
                thread.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
            }
            catch(InterruptedException e){
                Thread.currentThread().interrupt(); // set interrupt flag
            }
        }
    }

    public void run() {
        byte[] buf;
        DatagramPacket packet;
        while(!diag_sock.isClosed() && Thread.currentThread().equals(thread)) {
            buf=new byte[1500]; // requests are small (responses might be bigger)
            packet=new DatagramPacket(buf, 0, buf.length);
            try {
                diag_sock.receive(packet);
                int payloadStartOffset = 0;
                if(isAuthorizationRequired()){
                   payloadStartOffset = authorizeProbeRequest(packet);
                }
                handleDiagnosticProbe(packet.getSocketAddress(), diag_sock,
                                      new String(packet.getData(), packet.getOffset() + payloadStartOffset, packet.getLength()));
            }
            catch(IOException socket_ex) {
            }
            catch(Throwable e) {
                if(log.isErrorEnabled())
                    log.error("failure handling diagnostics request", e);
            }
        }
    }

    protected void handleDiagnosticProbe(SocketAddress sender, DatagramSocket sock, String request) {
        StringTokenizer tok=new StringTokenizer(request);
        List<String> list=new ArrayList<String>(10);

        while(tok.hasMoreTokens()) {
            String req=tok.nextToken().trim();
            if(!req.isEmpty())
                list.add(req);
        }

        String[] tokens=new String[list.size()];
        for(int i=0; i < list.size(); i++)
            tokens[i]=list.get(i);

        for(ProbeHandler handler: handlers) {
            Map<String, String> map=handler.handleProbe(tokens);
            if(map == null || map.isEmpty())
                continue;
            StringBuilder info=new StringBuilder();
            for(Map.Entry<String,String> entry: map.entrySet()) {
                info.append(entry.getKey()).append("=").append(entry.getValue()).append("\r\n");
            }

            byte[] diag_rsp=info.toString().getBytes();
            if(log.isDebugEnabled())
                log.debug("sending diag response to " + sender);
            try {
                sendResponse(sock, sender, diag_rsp);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed sending diag rsp to " + sender, t);
            }
        }
    }

    /**
    * Performs authorization on given DatagramPacket. 
    * 
    * @param packet to authorize
    * @return offset in DatagramPacket where request payload starts
    * @throws Exception thrown if passcode received from client does not match set passcode
    */
   protected int authorizeProbeRequest(DatagramPacket packet) throws Exception {
      int offset = 0;
      ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData());
      DataInputStream in = new DataInputStream(bis);
      long t1 = in.readLong();
      double q1 = in.readDouble();
      int length = in.readInt();
      byte[] digest = new byte[length];
      in.readFully(digest);
      offset = 8 + 8 + 4 + digest.length;

      byte[] local = Util.createDigest(passcode, t1, q1);
      if (!MessageDigest.isEqual(digest, local)) {
         throw new Exception("Authorization failed! Make sure correct passcode is used");
      } else {
         if(log.isDebugEnabled())
            log.debug("Request authorized");
      }
      return offset;
   }

   protected static void sendResponse(DatagramSocket sock, SocketAddress sender, byte[] buf) throws IOException {
        DatagramPacket p=new DatagramPacket(buf, 0, buf.length, sender);
        sock.send(p);
    }

    protected void bindToInterfaces(List<NetworkInterface> interfaces, MulticastSocket s) {
        SocketAddress group_addr=new InetSocketAddress(diagnostics_addr, diagnostics_port);
        for(Iterator<NetworkInterface> it=interfaces.iterator(); it.hasNext();) {
            NetworkInterface i=it.next();
            try {
                if (i.isUp() && !i.getInterfaceAddresses().isEmpty()) { // fix for VM crash - suggested by JJalenak@netopia.com
                    s.joinGroup(group_addr, i);
                    if(log.isTraceEnabled())
                        log.trace("joined " + group_addr + " on " + i.getName());
                }
            }
            catch(IOException e) {
                log.warn("failed to join " + group_addr + " on " + i.getName() + ": " + e);
            }
        }
    }
    
    protected boolean isAuthorizationRequired(){
       return passcode != null;
    }

    public interface ProbeHandler {
        /**
         * Handles a probe. For each key that is handled, the key and its result should be in the returned map.
         * @param keys
         * @return Map<String,String>. A map of keys and values. A null return value is permissible.
         */
        Map<String,String> handleProbe(String... keys);

        /** Returns a list of supported keys */
        String[] supportedKeys();
    }
}