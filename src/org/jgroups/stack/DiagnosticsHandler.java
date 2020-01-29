package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;
import org.jgroups.util.Runner;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;

/**
 * @author Bela Ban
 * @since 3.0
 */

public class DiagnosticsHandler extends ReceiverAdapter {
    public static final String        UDP_THREAD_NAME="UdpDiagHandler";
    public static final String        TCP_THREAD_NAME="TcpDiagHandler";
    protected TP                      transport;
    protected volatile boolean        udp_enabled=true, tcp_enabled=true;
    protected ServerSocket            srv_sock;  // when TCP is used
    protected Runner                  udp_runner, tcp_runner;
    protected MulticastSocket         udp_mcast_sock;        // receiving of mcast packets when UDP is used
    protected DatagramSocket          udp_ucast_sock;        // sending of UDP responses
    protected InetAddress             diagnostics_addr;      // multicast address for the UDP multicast socket
    protected InetAddress             diagnostics_bind_addr; // to bind the TCP socket
    protected int                     diagnostics_port=7500;
    protected int                     diagnostics_port_range=50;
    protected int                     ttl=8;
    protected List<NetworkInterface>  bind_interfaces;
    protected final Set<ProbeHandler> handlers=new CopyOnWriteArraySet<>();
    protected final Log               log;
    protected final SocketFactory     socket_factory;
    protected final ThreadFactory     thread_factory;
    protected final String            passcode;

    protected final BiConsumer<SocketAddress,String> udp_response_sender=
      (sender,response) -> sendResponse(udp_ucast_sock, sender, response.getBytes());

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
        this.passcode=passcode;
    }

    public DiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port,
                              List<NetworkInterface> bind_interfaces, int diagnostics_ttl,
                              Log log, SocketFactory socket_factory, ThreadFactory thread_factory, String passcode) {
        this(diagnostics_addr, diagnostics_port, log, socket_factory, thread_factory, passcode);
        this.bind_interfaces=bind_interfaces;
        this.ttl=diagnostics_ttl;
    }

    public TP                 transport()                              {return transport;}
    public DiagnosticsHandler transport(TP tp)                         {transport=tp; return this;}
    public DiagnosticsHandler setDiagnosticsBindAddress(InetAddress a) {this.diagnostics_bind_addr=a; return this;}
    public InetAddress        getDiagnosticsBindAddress()              {return diagnostics_bind_addr;}
    public boolean            udpEnabled()                             {return udp_enabled;}
    public DiagnosticsHandler enableUdp(boolean f)                     {udp_enabled=f; return this;}
    public boolean            tcpEnabled()                             {return tcp_enabled;}
    public DiagnosticsHandler enableTcp(boolean f)                     {tcp_enabled=f; return this;}
    public int                getDiagnosticsPortRange()                {return diagnostics_port_range;}
    public DiagnosticsHandler setDiagnosticsPortRange(int r)           {diagnostics_port_range=r; return this;}

    public DiagnosticsHandler setThreadNames() {
        if(udp_runner != null) {
            Thread tmp=udp_runner.getThread();
            if(tmp != null)
                thread_factory.renameThread(UDP_THREAD_NAME, tmp);
        }
        if(tcp_runner != null && tcp_runner.isRunning()) {
            Thread tmp=tcp_runner.getThread();
            if(tmp != null)
                thread_factory.renameThread(TCP_THREAD_NAME, tmp);
        }
        return this;
    }

    public DiagnosticsHandler unsetThreadNames() {
        if(udp_runner != null)
            udp_runner.threadName(UDP_THREAD_NAME);
        if(tcp_runner != null)
            tcp_runner.threadName(TCP_THREAD_NAME);
        return this;
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

    public void start() throws Exception {
        if(!udp_enabled && !tcp_enabled)
            throw new IllegalStateException("both UDP and TCP are disabled - enable at least 1 of them");
        if(udp_enabled)
            startUDP();
        if(tcp_enabled)
            startTCP();
    }

    public void stop() {
        Util.close(udp_runner, tcp_runner); // only stops if running
    }

    public boolean isRunning() {
        return udp_runner != null && udp_runner.isRunning() || tcp_runner != null && tcp_runner.isRunning();
    }

    protected void runUDP() {
        byte[] buf=new byte[10000]; // requests are small (responses might be bigger)
        DatagramPacket packet=new DatagramPacket(buf, 0, buf.length);
        try {
            udp_mcast_sock.receive(packet);
            int payloadStartOffset = 0;
            if(passcode != null)
                payloadStartOffset=authorizeProbeRequest(packet);
            handleDiagnosticProbe(packet.getSocketAddress(),
                                  new String(packet.getData(), packet.getOffset() + payloadStartOffset, packet.getLength()),
                                  udp_response_sender);
        }
        catch(IOException socket_ex) {
        }
        catch(Throwable e) {
            log.error(Util.getMessage("FailureHandlingDiagnosticsRequest"), e);
        }
    }

    protected void runTCP() {
        SocketAddress sender=null;
        try(Socket client_sock=srv_sock.accept();
            InputStream input=client_sock.getInputStream();
            OutputStream output=client_sock.getOutputStream();) {
            sender=client_sock.getRemoteSocketAddress();
            String request=Util.readLine(input);
            handleDiagnosticProbe(sender, request, (snd,response) -> {
                try {
                    output.write(response.getBytes());
                }
                catch(IOException e) {
                    log.error("%s: failed handling TCP probe request: %s", transport.getLocalAddress(), e.getMessage());
                }
            });
        }
        catch(SocketException sock_ex) {

        }
        catch(Throwable t) {
            log.error("%s: failed processing TCP client request from %s: %s", transport.getLocalAddress(), sender, t);
        }
    }

    protected DiagnosticsHandler startUDP() throws Exception {
        if(udp_ucast_sock == null || udp_ucast_sock.isClosed())
            udp_ucast_sock=socket_factory.createDatagramSocket("jgroups.tp.diag.udp_ucast_sock");

        if(udp_mcast_sock == null || udp_mcast_sock.isClosed()) {
            // https://jira.jboss.org/jira/browse/JGRP-777 - this doesn't work on MacOS, and we don't have
            // cross talking on Windows anyway, so we just do it for Linux. (How about Solaris ?)

            // If possible, the MulticastSocket(SocketAddress) ctor is used which binds to diagnostics_addr:diagnostics_port.
            // This acts like a filter, dropping multicasts to different multicast addresses
            if(Util.can_bind_to_mcast_addr)
                udp_mcast_sock=Util.createMulticastSocket(socket_factory, "jgroups.tp.diag.udp_mcast_sock",
                                                          diagnostics_addr, diagnostics_port, log);
            else
                udp_mcast_sock=socket_factory.createMulticastSocket("jgroups.tp.diag.udp_mcast_sock", diagnostics_port);
            try {
                udp_mcast_sock.setTimeToLive(ttl);
            }
            catch(Exception ex) {
                log.error("failed setting TTL %d in MulticastSocket: %s", ttl, ex.getMessage());
            }
            List<NetworkInterface> interfaces=bind_interfaces != null? bind_interfaces : Util.getAllAvailableInterfaces();
            bindToInterfaces(interfaces, udp_mcast_sock);
        }
        if(udp_runner == null)
            udp_runner=new Runner(thread_factory, UDP_THREAD_NAME, this::runUDP,
                                  () -> Util.close(udp_mcast_sock, udp_ucast_sock)).daemon(true);
        udp_runner.start();
        return this;
    }

    protected DiagnosticsHandler startTCP() throws Exception {
        if(srv_sock == null || srv_sock.isClosed())
            srv_sock=Util.createServerSocket(socket_factory, "jgroups.tp.diag.tcp_sock",
                                             diagnostics_bind_addr, diagnostics_port, diagnostics_port+diagnostics_port_range);
        if(tcp_runner == null)
            tcp_runner=new Runner(thread_factory, TCP_THREAD_NAME, this::runTCP, () -> Util.close(srv_sock));
        tcp_runner.start();
        return this;
    }


    protected void handleDiagnosticProbe(SocketAddress sender, String request,
                                         BiConsumer<SocketAddress,String> rsp_sender) {
        StringTokenizer tok=new StringTokenizer(request);
        List<String> list=new ArrayList<>(10);

        while(tok.hasMoreTokens()) {
            String req=tok.nextToken().trim();
            if(!req.isEmpty()) {
                 // if -cluster=<name>: discard requests that have a cluster name != our own cluster name
                if(req.startsWith("cluster=")) {
                    if(!sameCluster(req))
                        return;
                    continue;
                }
                list.add(req);
            }
        }
        if(list.isEmpty()) {
            if(transport != null) {
                Address local_addr=transport.localAddress();
                String default_rsp=String.format("local_addr=%s\nphysical_addr=%s\nview=%s\ncluster=%s\nversion=%s\n",
                                                 local_addr != null? local_addr : "n/a",
                                                 transport.getLocalPhysicalAddress(),
                                                 transport.view(),
                                                 transport.getClusterName(),
                                                 Version.description);
                rsp_sender.accept(sender, default_rsp);
            }
            return;
        }

        String[] tokens=new String[list.size()];
        for(int i=0; i < list.size(); i++)
            tokens[i]=list.get(i);

        for(ProbeHandler handler: handlers) {
            Map<String, String> map=null;
            try {
                map=handler.handleProbe(tokens);
            }
            catch(IllegalArgumentException ex) {
                log.warn(ex.getMessage());
                return;
            }
            if(map == null || map.isEmpty())
                continue;
            StringBuilder info=new StringBuilder(defaultHeaders());
            for(Map.Entry<String,String> entry: map.entrySet())
                info.append(String.format("%s=%s\r\n", entry.getKey(), entry.getValue()));

            String diag_rsp=info.toString();
            rsp_sender.accept(sender, diag_rsp);
        }
    }

    protected String defaultHeaders() {
        if(transport == null) return "";
        Address local_addr=transport.localAddress();
        View view=transport.view();
        int num_members=view != null? view.size() : 0;
        return String.format("local_addr=%s [ip=%s, version=%s, cluster=%s, %d mbr(s)]\n",
                             local_addr != null? local_addr : "n/a", transport.getLocalPhysicalAddress(),
                             Version.description, transport.getClusterName(), num_members);
    }


    protected boolean sameCluster(String req) {
        if(!req.startsWith("cluster="))
            return true;
        String cluster_name_pattern=req.substring("cluster=".length()).trim();
        String cname=transport.getClusterName();
        if(cluster_name_pattern != null && !Util.patternMatch(cluster_name_pattern, cname)) {
            log.debug("Probe request dropped as cluster name %s does not match pattern %s", cname, cluster_name_pattern);
            return false;
        }
        return true;
    }


    /**
     * Performs authorization on given DatagramPacket.
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
        if(!MessageDigest.isEqual(digest, local))
            throw new Exception("Authorization failed! Make sure correct passcode is used");
        return offset;
    }

    protected void sendResponse(DatagramSocket sock, SocketAddress sender, byte[] buf) {
        try {
            DatagramPacket p=new DatagramPacket(buf, 0, buf.length, sender);
            sock.send(p);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedSendingDiagRspTo") + sender, t);
        }
    }

    protected void bindToInterfaces(List<NetworkInterface> interfaces, MulticastSocket s) {
        SocketAddress group_addr=new InetSocketAddress(diagnostics_addr, diagnostics_port);
        for(Iterator<NetworkInterface> it=interfaces.iterator(); it.hasNext();) {
            NetworkInterface i=it.next();
            try {
                if(Util.isUp(i)) {
                    List<InterfaceAddress> inet_addrs=i.getInterfaceAddresses();
                    if(inet_addrs != null && !inet_addrs.isEmpty()) { // fix for VM crash - suggested by JJalenak@netopia.com
                        s.joinGroup(group_addr, i);
                        log.trace("joined %s on %s", group_addr, i.getName());
                    }
                }
            }
            catch(Exception e) { // also catches NPE in getInterfaceAddresses() (https://issues.jboss.org/browse/JGRP-1845)
                log.warn("failed to join " + group_addr + " on " + i.getName() + ": " + e);
            }
        }
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
