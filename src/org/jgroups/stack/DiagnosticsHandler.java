package org.jgroups.stack;

import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.logging.Log;
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
import java.util.function.Function;

/**
 * @author Bela Ban
 * @since 3.0
 */

public class DiagnosticsHandler extends ReceiverAdapter implements Closeable {
    public static final String         UDP_THREAD_NAME="UdpDiagHandler";
    public static final String         TCP_THREAD_NAME="TcpDiagHandler";

    @Property(description="Switch to enable diagnostic probing")
    protected boolean                  enabled=true;

    @Property(description="Use a multicast socket to listen for probe requests (ignored if diag.enabled is false)")
    protected volatile boolean         enable_udp=true;

    @Property(description="Use a TCP socket to listen for probe requests (ignored if diag.enabled is false)")
    protected volatile boolean         enable_tcp;

    @Property(description="Multicast address for diagnostic probing (UDP MulticastSocket). Used when enable_udp " +
      "is true", defaultValueIPv4="224.0.75.75",defaultValueIPv6="ff0e::0:75:75")
    protected InetAddress              mcast_addr;

    @Property(description="Port for diagnostic probing. Default is 7500")
    protected int                      port=7500;

    @Property(description="Bind address for diagnostic probing, to bind the TCP socket. Used when enable_tcp is true")
    protected InetAddress              bind_addr;

    @Property(description="The number of ports to be probed for an available port (TCP)")
    protected int                      port_range=50;

    @Property(description="TTL of the diagnostics multicast socket")
    protected int                      ttl=8;

    @Property(converter=PropertyConverters.NetworkInterfaceList.class,
      description="Comma delimited list of interfaces (IP addrs or interface names) that the multicast socket should bind to")
    protected List<NetworkInterface>   bind_interfaces;

    @Property(description="Authorization passcode for diagnostics. If specified every probe query will be authorized")
    protected String                   passcode;

    protected ServerSocket             srv_sock;  // when TCP is used
    protected Runner                   udp_runner, tcp_runner;
    protected MulticastSocket          udp_mcast_sock;        // receiving of mcast packets when UDP is used
    protected DatagramSocket           udp_ucast_sock;        // sending of UDP responses
    protected final Set<ProbeHandler>  handlers=new CopyOnWriteArraySet<>();
    protected final Log                log;
    protected final SocketFactory      socket_factory;
    protected final ThreadFactory      thread_factory;
    protected Function<Boolean,String> print_headers=b -> "";
    protected Function<String,Boolean> same_cluster=b -> true;

    protected final BiConsumer<SocketAddress,String> udp_response_sender=
      (sender,response) -> sendResponse(udp_ucast_sock, sender, response.getBytes());

    public DiagnosticsHandler printHeaders(Function<Boolean,String> f) {
        print_headers=Objects.requireNonNull(f);
        return this;
    }

    public DiagnosticsHandler sameCluster(Function<String,Boolean> f) {
        same_cluster=Objects.requireNonNull(f);
        return this;
    }




    public DiagnosticsHandler(Log log, SocketFactory socket_factory, ThreadFactory thread_factory) {
        this.log=log;
        this.socket_factory=socket_factory;
        this.thread_factory=thread_factory;
    }


    public boolean                isEnabled()                                 {return enabled;}
    public DiagnosticsHandler     setEnabled(boolean f)                       {enabled=f; return this;}
    public DiagnosticsHandler     setMcastAddress(InetAddress a)              {this.mcast_addr=a; return this;}
    public InetAddress            getMcastAddress()                           {return mcast_addr;}
    public DiagnosticsHandler     setBindAddress(InetAddress a)               {this.bind_addr=a; return this;}
    public InetAddress            getBindAddress()                            {return bind_addr;}
    public boolean                udpEnabled()                                {return enable_udp;}
    public DiagnosticsHandler     enableUdp(boolean f)                        {enable_udp=f; if(f) enabled=f; return this;}
    public boolean                tcpEnabled()                                {return enable_tcp;}
    public DiagnosticsHandler     enableTcp(boolean f)                        {enable_tcp=f; if(f) enabled=f; return this;}
    public int                    getPort()                                   {return port;}
    public DiagnosticsHandler     setPort(int p)                              {port=p; return this;}
    public int                    getPortRange()                              {return port_range;}
    public DiagnosticsHandler     setPortRange(int r)                         {port_range=r; return this;}
    public int                    getTtl()                                    {return ttl;}
    public DiagnosticsHandler     setTtl(int d)                               {this.ttl=d; return this;}
    public String                 getPasscode()                               {return passcode;}
    public DiagnosticsHandler     setPasscode(String d)                       {this.passcode=d; return this;}
    public List<NetworkInterface> getBindInterfaces()                         {return bind_interfaces;}
    public DiagnosticsHandler     setBindInterfaces(List<NetworkInterface> l) {this.bind_interfaces=l; return this;}


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

    public DiagnosticsHandler registerProbeHandler(ProbeHandler handler) {
        handlers.add(Objects.requireNonNull(handler));
        return this;
    }

    public DiagnosticsHandler unregisterProbeHandler(ProbeHandler handler) {
        if(handler != null)
            handlers.remove(handler);
        return this;
    }

    public DiagnosticsHandler start() throws Exception {
        if(!enabled)
            return this;
        if(!enable_udp && !enable_tcp)
            throw new IllegalStateException("both UDP and TCP are disabled - enable at least 1 of them");
        if(enable_udp)
            startUDP();
        if(enable_tcp)
            startTCP();
        return this;
    }

    public void close() throws IOException {
        stop();
    }

    public DiagnosticsHandler stop() {
        Util.close(udp_runner, tcp_runner);
        return this; // only stops if running
    }

    @ManagedAttribute(description="Is the diagnostics handler running?")
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
                    log.error("failed handling TCP probe request: %s", e.getMessage());
                }
            });
        }
        catch(SocketException sock_ex) {

        }
        catch(Throwable t) {
            log.error("failed processing TCP client request from %s: %s", sender, t);
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
                                                          mcast_addr, port, log);
            else
                udp_mcast_sock=socket_factory.createMulticastSocket("jgroups.tp.diag.udp_mcast_sock", port);
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
                                             bind_addr, port, port+ port_range, 0);
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
                    if(!same_cluster.apply(req)) {
                        log.debug("probe response dropped as cluster %s does not match", req.substring("cluster=".length()));
                        return;
                    }
                    continue;
                }
                list.add(req);
            }
        }
        if(list.isEmpty()) {
            String default_rsp=print_headers.apply(true);
            rsp_sender.accept(sender, default_rsp);
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
            String tmp=print_headers.apply(false);
            StringBuilder info=new StringBuilder(tmp);
            for(Map.Entry<String,String> entry: map.entrySet())
                info.append(String.format("%s=%s\r\n", entry.getKey(), entry.getValue()));

            String diag_rsp=info.toString();
            rsp_sender.accept(sender, diag_rsp);
        }
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
        SocketAddress group_addr=new InetSocketAddress(mcast_addr, port);
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
         * @return Map<String,String>. A map of keys and values. A null return value is permissible.
         */
        Map<String,String> handleProbe(String... keys);

        /** Returns a list of supported keys */
        String[] supportedKeys();
    }
}
