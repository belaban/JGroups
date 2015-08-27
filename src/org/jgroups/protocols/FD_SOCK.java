package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;


/**
 * Failure detection protocol based on sockets. Failure detection is ring-based. Each member creates a
 * server socket and announces its address together with the server socket's address in a multicast. A
 * pinger thread will be started when the membership goes above 1 and will be stopped when it drops below
 * 2. The pinger thread connects to its neighbor on the right and waits until the socket is closed. When
 * the socket is closed by the monitored peer in an abnormal fashion (IOException), the neighbor will be
 * suspected.<p> The main feature of this protocol is that no ping messages need to be exchanged between
 * any 2 peers, and failure detection relies entirely on TCP sockets. The advantage is that no activity
 * will take place between 2 peers as long as they are alive (i.e. have their server sockets open).
 * The disadvantage is that hung servers or crashed routers will not cause sockets to be closed, therefore
 * they won't be detected.
 * The FD_SOCK protocol will work for groups where members are on different hosts<p>
 * The costs involved are 2 additional threads: one that
 * monitors the client side of the socket connection (to monitor a peer) and another one that manages the
 * server socket. However, those threads will be idle as long as both peers are running.
 * @author Bela Ban May 29 2001
 */
@MBean(description="Failure detection protocol based on sockets connecting members")
public class FD_SOCK extends Protocol implements Runnable {
    protected static final int NORMAL_TERMINATION=9;
    protected static final int ABNORMAL_TERMINATION=-1;

    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @LocalAddress
    @Property(description="The NIC on which the ServerSocket should listen on. " +
      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress bind_addr=null;

    @Property(description="Use \"external_addr\" if you have hosts on different networks, behind " +
      "firewalls. On each firewall, set up a port forwarding rule (sometimes called \"virtual server\") to " +
      "the local IP (e.g. 192.168.1.100) of the host then on each host, set \"external_addr\" TCP transport " +
      "parameter to the external (public IP) address of the firewall.",
              systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress external_addr=null;

    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
              systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int         external_port=0;

    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class,
        description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr")
    protected String      bind_interface_str=null;

    @Property(description="Timeout for getting socket cache from coordinator. Default is 1000 msec")
    protected long        get_cache_timeout=1000;

    @Property(description="Interval for broadcasting suspect messages. Default is 5000 msec")
    protected long        suspect_msg_interval=5000;

    @Property(description="Number of attempts coordinator is solicited for socket cache until we give up. Default is 3")
    protected int         num_tries=3;

    @Property(description="Start port for server socket. Default value of 0 picks a random port")
    protected int         start_port=0;

    @Property(description="Start port for client socket. Default value of 0 picks a random port")
    protected int         client_bind_port=0;

    @Property(description="Number of ports to probe for start_port and client_bind_port")
    protected int         port_range=50;

    @Property(description="Whether to use KEEP_ALIVE on the ping socket or not. Default is true")
    protected boolean     keep_alive=true;

    @Property(description="Max time in millis to wait for ping Socket.connect() to return")
    protected int         sock_conn_timeout=1000;


    /* ---------------------------------------------   JMX      ------------------------------------------------------ */


    protected int num_suspect_events=0;

    protected final BoundedList<String> suspect_history=new BoundedList<>(20);


    /* --------------------------------------------- Fields ------------------------------------------------------ */


    protected volatile List<Address> members=new ArrayList<>(11); // volatile eliminates the lock

    protected final Set<Address>     suspected_mbrs=new CopyOnWriteArraySet<>();

    protected final List<Address>    pingable_mbrs=new CopyOnWriteArrayList<>();

    protected volatile boolean srv_sock_sent=false; // has own socket been broadcast yet ?
    /** Used to rendezvous on GET_CACHE and GET_CACHE_RSP */
    protected final Promise<Map<Address,IpAddress>> get_cache_promise=new Promise<>();
    protected volatile boolean got_cache_from_coord=false; // was cache already fetched ?
    protected Address local_addr=null; // our own address
    protected ServerSocket srv_sock=null; // server socket to which another member connects to monitor me

    protected ServerSocketHandler srv_sock_handler=null; // accepts new connections on srv_sock
    protected IpAddress srv_sock_addr=null; // pair of server_socket:port
    protected Address ping_dest=null; // address of the member we monitor
    protected Socket ping_sock=null; // socket to the member we monitor
    protected InputStream ping_input=null; // input stream of the socket to the member we monitor
    @GuardedBy("this")
    protected volatile Thread pinger_thread=null; // listens on ping_sock, suspects member if socket is closed

    /** Cache of member addresses and their ServerSocket addresses */
    protected final ConcurrentMap<Address,IpAddress> cache=Util.createConcurrentMap(11);

    protected final Promise<IpAddress> ping_addr_promise=new Promise<>(); // to fetch the ping_addr for ping_dest
    protected final Object sock_mutex=new Object(); // for access to ping_sock, ping_input
    protected TimeScheduler timer=null;
    protected final BroadcastTask bcast_task=new BroadcastTask(); // to transmit SUSPECT message (until view change)
    protected volatile boolean regular_sock_close=false; // used by interruptPingerThread() when new ping_dest is computed
    protected volatile boolean shuttin_down;
    protected boolean log_suspected_msgs=true;


    public FD_SOCK() {
    }

    @ManagedAttribute(description="Member address")
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute(description="List of cluster members")
    public String getMembers() {return Util.printListWithDelimiter(members, ",");}
    @ManagedAttribute(description="List of pingable members of a cluster")
    public String getPingableMembers() {return pingable_mbrs.toString();}
    @ManagedAttribute(description="List of currently suspected members")
    public String getSuspectedMembers() {return suspected_mbrs.toString();}
    @ManagedAttribute(description="The number of currently suspected members")
    public int getNumSuspectedMembers() {return suspected_mbrs.size();}
    @ManagedAttribute(description="Ping destination")
    public String getPingDest() {return ping_dest != null? ping_dest.toString() : "null";}
    @ManagedAttribute(description="Number of suspect event generated")
    public int getNumSuspectEventsGenerated() {return num_suspect_events;}
    @ManagedAttribute(description="Whether the node crash detection monitor is running",writable=false)
    public boolean isNodeCrashMonitorRunning() {return isPingerThreadRunning(); }

    public boolean isLogSuspectedMessages() {
        return log_suspected_msgs;
    }

    public void setLogSuspectedMessages(boolean log_suspected_msgs) {
        this.log_suspected_msgs=log_suspected_msgs;
    }

    @ManagedOperation(description="Print suspect history")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(String suspect: suspect_history)
            sb.append(suspect).append("\n");
        return sb.toString();
    }

    @ManagedOperation
    public String printCache() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,IpAddress> entry: cache.entrySet()) {
            sb.append(entry.getKey()).append(" has server socket at ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Starts node crash monitor if member count > 1 and monitor is not running")
    public boolean startNodeCrashMonitor() {
        if(members.size() > 1) {
            if(startPingerThread()) {
                log.warn("Node crash detection manually started, was not running for some reason.");
                return true;
            }
            log.debug("Node crash detection is already running.");
        }
        else
            log.debug("Single node cluster, no need for node crash detection.");
        return false;
    }

    public void init() throws Exception {
        shuttin_down=false;
        srv_sock_handler=new ServerSocketHandler();
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
    }


    public void start() throws Exception {
        shuttin_down=false; super.start();
    }

    public void stop() {
        shuttin_down=true;
        pingable_mbrs.clear();
        stopPingerThread();
        stopServerSocket(true); // graceful close
        bcast_task.removeAll();
        suspected_mbrs.clear();
    }

    public void resetStats() {
        super.resetStats();
        num_suspect_events=0;
        suspect_history.clear();
    }


    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message) evt.getArg();
                FdHeader hdr=(FdHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break;  // message did not originate from FD_SOCK layer, just pass up

                switch(hdr.type) {

                    case FdHeader.SUSPECT:
                        if(hdr.mbrs != null) {
                            log.trace("%s: received SUSPECT message from %s: suspects=%s", local_addr, msg.getSrc(), hdr.mbrs);
                            suspect(hdr.mbrs);
                        }
                        break;

                    case FdHeader.UNSUSPECT:
                        if(hdr.mbrs != null) {
                            log.trace("%s: received UNSUSPECT message from %s: mbrs=%s", local_addr, msg.getSrc(), hdr.mbrs);
                            for(Address tmp: hdr.mbrs)
                                unsuspect(tmp);
                        }
                        break;

                    // If I have the sock for 'hdr.mbr', return it. Otherwise look it up in my cache and return it
                    case FdHeader.WHO_HAS_SOCK:
                        if(local_addr != null && local_addr.equals(msg.getSrc()))
                            return null; // don't reply to WHO_HAS bcasts sent by me !

                        if(hdr.mbr == null)
                            return null;

                        log.trace("%s: who-has-sock %s", local_addr, hdr.mbr);

                        // 1. Try my own address, maybe it's me whose socket is wanted
                        if(local_addr != null && local_addr.equals(hdr.mbr) && srv_sock_addr != null) {
                            sendIHaveSockMessage(msg.getSrc(), local_addr, srv_sock_addr);  // unicast message to msg.getSrc()
                            return null;
                        }

                        // 2. If I don't have it, maybe it is in the cache
                        IpAddress addr=cache.get(hdr.mbr);
                        if(addr != null)
                            sendIHaveSockMessage(msg.getSrc(), hdr.mbr, addr);  // ucast msg
                        break;


                    // Update the cache with the addr:sock_addr entry (if on the same host)
                    case FdHeader.I_HAVE_SOCK:
                        if(hdr.mbr == null || hdr.sock_addr == null)
                            return null;

                        // if(!cache.containsKey(hdr.mbr))
                        cache.put(hdr.mbr, hdr.sock_addr); // update the cache
                        log.trace("%s: i-have-sock: %s --> %s (cache is %s)", local_addr, hdr.mbr, hdr.sock_addr, cache);

                        if(ping_dest != null && hdr.mbr.equals(ping_dest))
                            ping_addr_promise.setResult(hdr.sock_addr);
                        break;

                    // Return the cache to the sender of this message
                    case FdHeader.GET_CACHE:
                        msg=new Message(msg.getSrc()).setFlag(Message.Flag.INTERNAL)
                          .putHeader(this.id, new FdHeader(FdHeader.GET_CACHE_RSP)).setBuffer(marshal(cache));
                        down_prot.down(new Event(Event.MSG, msg));
                        break;

                    case FdHeader.GET_CACHE_RSP:
                        Map<Address,IpAddress> cachedAddrs=readAddresses(msg.getRawBuffer(),msg.getOffset(),msg.getLength());
                        if(cachedAddrs != null)
                            get_cache_promise.setResult(cachedAddrs);
                        break;
                }
                return null;

            case Event.CONFIG:
                Map<String,Object> config=(Map<String,Object>)evt.getArg();
                if(bind_addr == null)
                    bind_addr=(InetAddress)config.get("bind_addr");
                if(external_addr == null)
                    external_addr=(InetAddress)config.get("external_addr");
                if(external_port <= 0) {
                    Object val=config.get("external_port");
                    if(val != null)
                        external_port=(Integer)val;
                }
                break;
        }

        return up_prot.up(evt);                                        // pass up to the layer above us
    }


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.UNSUSPECT:
                broadcastUnuspectMessage((Address)evt.getArg());
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                shuttin_down=false;
                Object ret=down_prot.down(evt);
                try {
                    startServerSocket();
                }
                catch(Exception e) {
                    throw new IllegalArgumentException("failed to start server socket", e);
                }
                return ret;

            case Event.DISCONNECT:
                shuttin_down=true;
                stopServerSocket(true); // graceful close
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address) evt.getArg();
                break;

            case Event.VIEW_CHANGE:
                View v=(View) evt.getArg();
                final List<Address> new_mbrs=v.getMembers();

                members=new_mbrs;  // volatile write will ensure all reads after this see the new membership
                suspected_mbrs.retainAll(new_mbrs);
                cache.keySet().retainAll(new_mbrs); // remove all entries in 'cache' which are not in the new membership
                bcast_task.adjustSuspectedMembers(new_mbrs);
                pingable_mbrs.clear();
                pingable_mbrs.addAll(new_mbrs);
                log.debug("%s: VIEW_CHANGE received: %s", local_addr, new_mbrs);

                if(new_mbrs.size() > 1) {
                    Address tmp_ping_dest=determinePingDest();
                    boolean hasNewPingDest = tmp_ping_dest != null && !tmp_ping_dest.equals(ping_dest);
                    if(hasNewPingDest) {
                        interruptPingerThread(false); // allows the thread to use the new socket
                        startPingerThread(); // in case it wasn't running; only starts if not yet running
                    }
                }
                else {
                    ping_dest=null;
                    stopPingerThread();
                }
                break;

            default:
                return down_prot.down(evt);
        }

        return down_prot.down(evt);
    }


    /**
     * Runs as long as there are 2 members and more. Determines the member to be monitored and fetches its
     * server socket address (if n/a, sends a message to obtain it). The creates a client socket and listens on
     * it until the connection breaks. If it breaks, emits a SUSPECT message. It the connection is closed regularly,
     * nothing happens. In both cases, a new member to be monitored will be chosen and monitoring continues (unless
     * there are fewer than 2 members).
     */
    public void run() {

        // 1. Broadcast my own addr:sock to all members so they can update their cache
        if(!srv_sock_sent) {
            if(srv_sock_addr != null) {
                sendIHaveSockMessage(null, // send to all members
                                     local_addr,
                                     srv_sock_addr);
                srv_sock_sent=true;
            }
        }

        // 2. Get the addr:pid cache from the coordinator (only if not already fetched)
        if(!got_cache_from_coord) {
            getCacheFromCoordinator();
            got_cache_from_coord=true;
        }

        log.trace("%s: pinger_thread started", local_addr);
        while(pingable_mbrs != null && !pingable_mbrs.isEmpty()) {
            regular_sock_close=false;
            ping_dest=determinePingDest(); // gets the neighbor to our right

            log.debug("%s: ping_dest is %s, pingable_mbrs=%s", local_addr, ping_dest, pingable_mbrs);

            if(ping_dest == null || !isPingerThreadRunning())
                break;

            IpAddress ping_addr=fetchPingAddress(ping_dest);

            if(ping_addr == null) {
                log.trace("%s: socket address for %s could not be fetched, retrying", local_addr, ping_dest);
                Util.sleep(1000);
                continue;
            }

            if(!setupPingSocket(ping_addr) && isPingerThreadRunning()) {
                // covers use cases #7 and #8 in ManualTests.txt
                log.debug("%s: could not create socket to %s (pinger thread is %s)", local_addr, ping_dest, isPingerThreadRunning()? "running" : "not running");
                broadcastSuspectMessage(ping_dest);
                pingable_mbrs.remove(ping_dest);
                continue;
            }

            log.trace("%s: ping_dest=%s, ping_sock=%s, cache=%s", local_addr, ping_dest, ping_sock, cache);

            // at this point ping_input must be non-null, otherwise setupPingSocket() would have thrown an exception
            try {
                if(ping_input != null) {
                    int c=ping_input.read();
                    switch(c) {
                        case NORMAL_TERMINATION:
                            log.debug("%s: peer %s closed socket gracefully", local_addr, ping_dest);
                            pingable_mbrs.remove(ping_dest);
                            break;
                        case ABNORMAL_TERMINATION: // -1 means EOF
                            handleSocketClose(null);
                            break;
                        default:
                            break;
                    }
                }
            }
            catch(IOException ex) {  // we got here when the peer closed the socket --> suspect peer and then continue
                handleSocketClose(ex);
            }
            catch(Throwable catch_all_the_rest) {
                log.error("exception", catch_all_the_rest);
            }
        }
        log.trace("%s: pinger thread terminated", local_addr);
    }

    protected synchronized boolean isPingerThreadRunning() {
        return pinger_thread != null;
    }




    /* ----------------------------------- Private Methods -------------------------------------- */


    protected void suspect(Set<Address> suspects) {
        if(suspects == null)
            return;

        suspects.remove(local_addr);
        final List<Address> eligible_mbrs=new ArrayList<>();
        for(Address suspect: suspects)
            suspect_history.add(String.format("%s: %s", new Date(), suspect));
        suspected_mbrs.addAll(suspects);
        eligible_mbrs.addAll(this.members);
        eligible_mbrs.removeAll(suspected_mbrs);

        // Check if we're coord, then send up the stack
        if(local_addr != null && !eligible_mbrs.isEmpty()) {
            Address first=eligible_mbrs.get(0);
            if(local_addr.equals(first)) {
                log.debug("%s: suspecting %s", local_addr, suspected_mbrs);
                for(Address suspect: suspects) {
                    up_prot.up(new Event(Event.SUSPECT, suspect));
                    down_prot.down(new Event(Event.SUSPECT, suspect));
                }
            }
        }
    }

    protected void unsuspect(Address mbr) {
        if(mbr == null)
            return;
        suspected_mbrs.remove(mbr);
        bcast_task.removeSuspectedMember(mbr);
    }

    protected void handleSocketClose(Exception ex) {
        teardownPingSocket();     // make sure we have no leftovers
        if(!regular_sock_close) { // only suspect if socket was not closed regularly (by interruptPingerThread())
            log.debug("%s: peer %s closed socket (%s)", local_addr, ping_dest, (ex != null ? ex.toString() : "eof"));
            broadcastSuspectMessage(ping_dest);
            pingable_mbrs.remove(ping_dest);
        }
        else {
            log.debug("%s: socket to %s was closed gracefully", local_addr, ping_dest);
            regular_sock_close=false;
        }
    }


    /**
     * Does *not* need to be synchronized on pinger_mutex because the caller (down()) already has the mutex acquired
     */
    protected synchronized boolean startPingerThread() {
        if(!isPingerThreadRunning()) {
            ThreadFactory factory=getThreadFactory();
            pinger_thread=factory.newThread(this, "FD_SOCK pinger");
            pinger_thread.setDaemon(true);
            pinger_thread.start();
            return true;
        }
        return false;
    }

    /**
     * Interrupts the pinger thread. The Thread.interrupt() method doesn't seem to work under Linux with JDK 1.3.1
     * (JDK 1.2.2 had no problems here), therefore we close the socket (setSoLinger has to be set !) if we are
     * running under Linux. This should be tested under Windows. (Solaris 8 and JDK 1.3.1 definitely works).<p>
     * Oct 29 2001 (bela): completely removed Thread.interrupt(), but used socket close on all OSs. This makes this
     * code portable and we don't have to check for OSs.<p/>
     */
    protected synchronized void interruptPingerThread(boolean sendTerminationSignal) {
        if(isPingerThreadRunning()) {
            regular_sock_close=true;
            if (sendTerminationSignal) {
                sendPingTermination();  // PATCH by Bruce Schuchardt (http://jira.jboss.com/jira/browse/JGRP-246)
            }
            teardownPingSocket(); // will wake up the pinger thread. less elegant than Thread.interrupt(), but does the job
        }
    }

    protected synchronized void stopPingerThread() {
        ping_addr_promise.setResult(null);
        get_cache_promise.setResult(null);

        interruptPingerThread(true);

        if(pinger_thread != null) {
            try {
                pinger_thread.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
            }
            catch(InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            pinger_thread=null;
        }

    }

    // PATCH: send something so the connection handler can exit
    protected void sendPingTermination() {
        sendPingSignal(NORMAL_TERMINATION);
    }


    protected void sendPingSignal(int signal) {
        synchronized(sock_mutex) {
            if(ping_sock != null) {
                try {
                    OutputStream out=ping_sock.getOutputStream();
                    if(out != null) {
                        out.write(signal);
                        out.flush();
                    }
                }
                catch(Throwable t) {
                    log.trace("%s: problem sending signal %s: %s", local_addr, signalToString(signal), t);
                }
            }
        }
    }






    protected void startServerSocket() throws Exception {
        srv_sock=Util.createServerSocket(getSocketFactory(),
                                         "jgroups.fd_sock.srv_sock", bind_addr, start_port, start_port+port_range); // grab a random unused port above 10000
        srv_sock_addr=new IpAddress(external_addr != null? external_addr : bind_addr, external_port > 0? external_port : srv_sock.getLocalPort());
        if(srv_sock_handler != null) {
            srv_sock_handler.start(); // won't start if already running
        }
    }

    public void stopServerSocket(boolean graceful) {
        if(srv_sock_handler != null)
            srv_sock_handler.stop(graceful);
    }


    /**
     * Creates a socket to <code>dest</code>, and assigns it to ping_sock. Also assigns ping_input
     */
    protected boolean setupPingSocket(IpAddress dest) {
        synchronized(sock_mutex) {
            if(dest == null) {
                return false;
            }
            SocketAddress destAddr=null;
            try {
                destAddr=new InetSocketAddress(dest.getIpAddress(), dest.getPort());
                // ping_sock=new Socket();
                ping_sock=getSocketFactory().createSocket("jgroups.fd.ping_sock");

                int num_bind_attempts=0;
                int port=client_bind_port;
                for(;;) {
                    try {
                        ping_sock.bind(new InetSocketAddress(bind_addr, port));
                        break;
                    }
                    catch(IOException e) {
                        if(++num_bind_attempts > port_range) {
                            log.error("%s: failed creating client socket to %s: %s", local_addr, dest, e);
                            throw e;
                        }
                        port++;
                    }
                }

                ping_sock.setSoLinger(true, 1);
                ping_sock.setKeepAlive(keep_alive);
                Util.connect(ping_sock, destAddr, sock_conn_timeout);
                ping_input=ping_sock.getInputStream();
                return true;
            }
            catch(Throwable ex) {
                if(!shuttin_down)
                    log.warn("%s: creating the client socket to %s failed: %s", local_addr, destAddr, ex.getMessage());
                return false;
            }
        }
    }


    protected void teardownPingSocket() {
        synchronized(sock_mutex) {
            if(ping_sock != null) {
                try {
                    ping_sock.shutdownInput();
                    ping_sock.close();
                }
                catch(Exception ex) {
                }
                ping_sock=null;
            }
            Util.close(ping_input);
            ping_input=null;
        }
    }


    /**
     * Determines coordinator C. If C is null and we are the first member, return. Else loop: send GET_CACHE message
     * to coordinator and wait for GET_CACHE_RSP response. Loop until valid response has been received.
     */
    protected void getCacheFromCoordinator() {
        Address coord;
        int attempts=num_tries;
        Message msg;
        FdHeader hdr;
        Map<Address,IpAddress> result;

        get_cache_promise.reset();
        while(attempts > 0 && isPingerThreadRunning()) {
            if((coord=determineCoordinator()) != null) {
                if(coord.equals(local_addr)) { // we are the first member --> empty cache
                    return;
                }
                hdr=new FdHeader(FdHeader.GET_CACHE);
                msg=new Message(coord).setFlag(Message.Flag.INTERNAL).putHeader(this.id, hdr);
                down_prot.down(new Event(Event.MSG, msg));
                result=get_cache_promise.getResult(get_cache_timeout);
                if(result != null) {
                    cache.putAll(result);
                    log.trace("%s: got cache from %s: cache is %s", local_addr, coord, cache);
                    return;
                }
            }

            --attempts;
        }
    }


    /**
     * Sends a SUSPECT message to all group members. Only the coordinator (or the next member in line if the coord
     * itself is suspected) will react to this message by installing a new view. To overcome the unreliability
     * of the SUSPECT message (it may be lost because we are not above any retransmission layer), the following scheme
     * is used: after sending the SUSPECT message, it is also added to the broadcast task, which will periodically
     * re-send the SUSPECT until a view is received in which the suspected process is not a member anymore. The reason is
     * that - at one point - either the coordinator or another participant taking over for a crashed coordinator, will
     * react to the SUSPECT message and issue a new view, at which point the broadcast task stops.
     */
    protected void broadcastSuspectMessage(Address suspected_mbr) {
        if(suspected_mbr == null) return;

        log.debug("%s: suspecting %s", local_addr, suspected_mbr);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        FdHeader hdr=new FdHeader(FdHeader.SUSPECT);
        hdr.mbrs=new HashSet<>(1);
        hdr.mbrs.add(suspected_mbr);
        Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, suspect_msg));

        // 2. Add to broadcast task and start latter (if not yet running). The task will end when
        //    suspected members are removed from the membership
        bcast_task.addSuspectedMember(suspected_mbr);
        if(stats) {
            num_suspect_events++;
            suspect_history.add(String.format("%s: %s", new Date(), suspected_mbr));
        }
    }


    protected void broadcastUnuspectMessage(Address mbr) {
        if(mbr == null) return;

        log.debug("%s: unsuspecting %s", local_addr, mbr);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        FdHeader hdr=new FdHeader(FdHeader.UNSUSPECT);
        hdr.mbrs=new HashSet<>(1);
        hdr.mbrs.add(mbr);
        Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, suspect_msg));
    }



    /**
     Sends or broadcasts a I_HAVE_SOCK response. If 'dst' is null, the reponse will be broadcast, otherwise
     it will be unicast back to the requester
     */
    protected void sendIHaveSockMessage(Address dst, Address mbr, IpAddress addr) {
        Message msg=new Message(dst).setFlag(Message.Flag.INTERNAL);
        FdHeader hdr=new FdHeader(FdHeader.I_HAVE_SOCK);
        hdr.mbr=mbr;
        hdr.sock_addr=addr;
        msg.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }


    /**
     Attempts to obtain the ping_addr first from the cache, then by unicasting q request to <code>mbr</code>,
     then by multicasting a request to all members.
     */
    protected IpAddress fetchPingAddress(Address mbr) {
        IpAddress ret;
        Message ping_addr_req;
        FdHeader hdr;

        if(mbr == null) {
            return null;
        }
        // 1. Try to get the server socket address from the cache
        if((ret=cache.get(mbr)) != null)
            return ret;

        if(!isPingerThreadRunning()) return null;

        // 2. Try to get the server socket address from mbr
        ping_addr_promise.reset();
        ping_addr_req=new Message(mbr).setFlag(Message.Flag.INTERNAL);
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        ping_addr_req.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, ping_addr_req));
        ret=ping_addr_promise.getResult(500);
        if(ret != null) {
            return ret;
        }

        if(!isPingerThreadRunning()) return null;

        // 3. Try to get the server socket address from all members
        ping_addr_req=new Message(null).setFlag(Message.Flag.INTERNAL);
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        ping_addr_req.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, ping_addr_req));
        ret=ping_addr_promise.getResult(500);
        return ret;
    }


    protected Address determinePingDest() {
        Address first_mbr = null;
        boolean several_mbrs = false;
        boolean found_local_addr = false;

        if(pingable_mbrs == null || local_addr == null)
            return null;

        // Look for the pingable member who follows the local_addr
        for(Address tmp: pingable_mbrs) {
            if(found_local_addr)
                return tmp;

            if(first_mbr == null) {
                first_mbr = tmp;
            }
            else {
              several_mbrs = true;
            }

            if (tmp.equals(local_addr))
                found_local_addr = true;
        }

        // If the local address was the last in the list, then wrap.
        if (found_local_addr && several_mbrs)
            return first_mbr;

        return null;
    }

    public static Buffer marshal(Map<Address,IpAddress> addrs) {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        try {
            int size=addrs != null? addrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Map.Entry<Address,IpAddress> entry: addrs.entrySet()) {
                    Address key=entry.getKey();
                    IpAddress val=entry.getValue();
                    Util.writeAddress(key, out);
                    Util.writeStreamable(val, out);
                }
            }
            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected Map<Address,IpAddress> readAddresses(byte[] buffer, int offset, int length) {
        if(buffer == null) return null;
        DataInput in=new ByteArrayDataInputStream(buffer, offset, length);
        HashMap<Address,IpAddress> addrs=null;
        try {
            int size=in.readInt();
            if(size > 0) {
                addrs=new HashMap<>(size);
                for(int i=0; i < size; i++) {
                    Address key=Util.readAddress(in);
                    IpAddress val=(IpAddress)Util.readStreamable(IpAddress.class, in);
                    addrs.put(key, val);
                }
            }
            return addrs;
        }
        catch(Exception ex) {
            log.error("%s: failed reading addresses from message: %s", local_addr, ex);
            return null;
        }
    }



    protected Address determineCoordinator() {
        List<Address> tmp=members;
        return !tmp.isEmpty()? tmp.get(0) : null;
    }


    protected static String signalToString(int signal) {
        switch(signal) {
            case NORMAL_TERMINATION: return "NORMAL_TERMINATION";
            case ABNORMAL_TERMINATION: return "ABNORMAL_TERMINATION";
            default: return "n/a";
        }
    }




    /* ------------------------------- End of Private Methods ------------------------------------ */


    public static class FdHeader extends Header {
        public static final byte SUSPECT       = 10;
        public static final byte UNSUSPECT     = 11;
        public static final byte WHO_HAS_SOCK  = 12;
        public static final byte I_HAVE_SOCK   = 13;
        public static final byte GET_CACHE     = 14; // sent by joining member to coordinator
        public static final byte GET_CACHE_RSP = 15; // sent by coordinator to joining member in response to GET_CACHE


        byte                      type=SUSPECT;
        Address                   mbr;           // set on WHO_HAS_SOCK (requested mbr), I_HAVE_SOCK
        IpAddress                 sock_addr;     // set on I_HAVE_SOCK
        Set<Address>              mbrs;          // set on SUSPECT (list of suspected members)


        public FdHeader() {
        }

        public FdHeader(byte type) {
            this.type=type;
        }

        public FdHeader(byte type, Address mbr) {
            this.type=type;
            this.mbr=mbr;
        }

        public FdHeader(byte type, Address mbr, IpAddress sock_addr) {
            this.type=type;
            this.mbr=mbr;
            this.sock_addr=sock_addr;
        }

        public FdHeader(byte type, Set<Address> mbrs) {
            this.type=type;
            this.mbrs=mbrs;
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type2String(type));
            if(mbr != null)
                sb.append(", mbr=").append(mbr);
            if(sock_addr != null)
                sb.append(", sock_addr=").append(sock_addr);
            if(mbrs != null)
                sb.append(", mbrs=").append(mbrs);
            return sb.toString();
        }


        public static String type2String(byte type) {
            switch(type) {
                case SUSPECT:       return "SUSPECT";
                case UNSUSPECT:     return "UNSUSPECT";
                case WHO_HAS_SOCK:  return "WHO_HAS_SOCK";
                case I_HAVE_SOCK:   return "I_HAVE_SOCK";
                case GET_CACHE:     return "GET_CACHE";
                case GET_CACHE_RSP: return "GET_CACHE_RSP";
                default:            return "unknown type (" + type + ')';
            }
        }


        public int size() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Util.size(mbr);

            // use of Util.size(Address) with IpAddress overestimates size by one byte.
            // replace: retval+=Util.size(sock_addr); with the following:
            int ipaddr_size = 0 ;
            ipaddr_size += Global.BYTE_SIZE ;     // presence byte
            if (sock_addr != null)
              ipaddr_size += sock_addr.size();  // IpAddress size
            retval += ipaddr_size ;
            retval+=Global.INT_SIZE; // mbrs size
            if(mbrs != null)
                for(Address m: mbrs)
                    retval+=Util.size(m);
            return retval;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeAddress(mbr, out);
            Util.writeStreamable(sock_addr, out);
            int size=mbrs != null? mbrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Address address: mbrs) {
                    Util.writeAddress(address, out);
                }
            }
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            mbr=Util.readAddress(in);
            sock_addr=(IpAddress)Util.readStreamable(IpAddress.class, in);
            int size=in.readInt();
            if(size > 0) {
                if(mbrs == null)
                    mbrs=new HashSet<>();
                for(int i=0; i < size; i++)
                    mbrs.add(Util.readAddress(in));
            }
        }

    }


    /**
     * Handles the server-side of a client-server socket connection. Waits until a client connects, and then loops
     * until that client closes the connection. Note that there is no new thread spawned for the listening on the
     * client socket, therefore there can only be 1 client connection at the same time. Subsequent clients attempting
     * to create a connection will be blocked until the first client closes its connection. This should not be a problem
     * as the ring nature of the FD_SOCK protocol always has only 1 client connect to its right-hand-side neighbor.
     */
    protected class ServerSocketHandler implements Runnable {
        Thread acceptor=null;
        /** List<ClientConnectionHandler> */
        final List<ClientConnectionHandler> clients=new LinkedList<>();


        String getName() {
            return acceptor != null? acceptor.getName() : null;
        }

        ServerSocketHandler() {
            start();
        }

        final void start() {
            if(acceptor == null) {
                acceptor=getThreadFactory().newThread(this, "FD_SOCK acceptor");
                acceptor.setDaemon(true);
                acceptor.start();
            }
        }


        final void stop(boolean graceful) {
            if(acceptor != null && acceptor.isAlive()) {
                try {
                    // this will terminate thread, peer will receive SocketException (socket close)
                    getSocketFactory().close(srv_sock);
                }
                catch(Exception ex) {
                }
            }
            synchronized(clients) {
                for(ClientConnectionHandler handler: clients) {
                    handler.stopThread(graceful);
                }
                clients.clear();
            }
            acceptor=null;
        }


        /** Only accepts 1 client connection at a time (saving threads) */
        public void run() {
            Socket client_sock;
            while(acceptor != null && srv_sock != null) {
                try {
                    client_sock=srv_sock.accept();
                    log.trace("%s: accepted connection from %s:%s",
                              local_addr, client_sock.getInetAddress(), client_sock.getPort());
                    client_sock.setKeepAlive(keep_alive);
                    ClientConnectionHandler client_conn_handler=new ClientConnectionHandler(client_sock, clients);
                    Thread t = getThreadFactory().newThread(client_conn_handler, "FD_SOCK client connection handler");
                    t.setDaemon(true);

                    synchronized(clients) {
                        clients.add(client_conn_handler);
                    }
                    t.start();
                }
                catch(IOException io_ex2) {
                    break;
                }
            }
            acceptor=null;
        }
    }



    /** Handles a client connection; multiple client can connect at the same time */
    protected static class ClientConnectionHandler implements Runnable {
        Socket                              client_sock=null;
        InputStream                         in;
        final Object                        mutex=new Object();
        final List<ClientConnectionHandler> clients;

        ClientConnectionHandler(Socket client_sock, List<ClientConnectionHandler> clients) {
            this.client_sock=client_sock;
            this.clients=clients;
        }

        void stopThread(boolean graceful) {
            synchronized(mutex) {
                if(client_sock != null) {
                    try {
                        if(graceful) {
                            OutputStream out=client_sock.getOutputStream();
                            out.write(NORMAL_TERMINATION);
                            out.flush();
                        }
                        closeClientSocket();
                    }
                    catch(Throwable t) {
                    }
                }
            }
        }

        protected void closeClientSocket() {
            synchronized(mutex) {
                Util.close(client_sock);
                client_sock=null;
            }
        }

        public void run() {
            try {
                synchronized(mutex) {
                    if(client_sock == null)
                        return;
                    in=client_sock.getInputStream();
                }
                int b;
                do {
                    b=in.read();
                }
                while(b != ABNORMAL_TERMINATION && b != NORMAL_TERMINATION);
            }
            catch(IOException ex) {
            }
            finally {
                Socket sock=client_sock; // PATCH: avoid race condition causing NPE
                if (sock != null && !sock.isClosed())
                    closeClientSocket();
                synchronized(clients) {
                    clients.remove(this);
                }
            }
        }
    }


    /**
     * Task that periodically broadcasts a list of suspected members to the group. Goal is not to lose
     * a SUSPECT message: since these are bcast unreliably, they might get dropped. The BroadcastTask makes
     * sure they are retransmitted until a view has been received which doesn't contain the suspected members
     * any longer. Then the task terminates.
     */
    protected class BroadcastTask implements Runnable {
        final Set<Address>    suspects=new HashSet<>();
        Future<?>             future;


        /** Adds a suspected member. Starts the task if not yet running */
        public void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            if(!members.contains(mbr)) return;
            synchronized(suspects) {
                if(suspects.add(mbr))
                    startTask();
            }
        }


        public void removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return;
            synchronized(suspects) {
                if(suspects.remove(suspected_mbr) && suspects.isEmpty())
                    stopTask();
            }
        }


        public void removeAll() {
            synchronized(suspects) {
                suspects.clear();
                stopTask();
            }
        }


        protected void startTask() {
            if(future == null || future.isDone()) {
                try {
                    future=timer.scheduleWithFixedDelay(this, suspect_msg_interval, suspect_msg_interval, TimeUnit.MILLISECONDS);
                }
                catch(RejectedExecutionException e) {
                    log.warn("%s: task %s was rejected as timer thread pool is shutting down", local_addr, this);
                }
            }
        }

        protected void stopTask() {
            if(future != null) {
                future.cancel(false);
                future=null;
            }
        }


        /**
         * Removes all elements from suspected_mbrs that are <em>not</em> in the new membership
         */
        public void adjustSuspectedMembers(List<Address> new_mbrship) {
            if(new_mbrship == null || new_mbrship.isEmpty()) return;
            synchronized(suspects) {
                boolean modified=suspects.retainAll(new_mbrship);
                if(modified)
                    log.trace("%s: adjusted suspected_mbrs: %s", local_addr, suspects);
                if(suspects.isEmpty())
                    stopTask();
            }
        }


        public void run() {
            log.trace("%s: broadcasting SUSPECT message (suspected_mbrs=%s)", local_addr, suspects);
            FdHeader hdr;
            synchronized(suspects) {
                if(suspects.isEmpty()) {
                    stopTask();
                    return;
                }

                hdr=new FdHeader(FdHeader.SUSPECT);
                hdr.mbrs=new HashSet<>(suspects);
            }
            Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(id, hdr); // mcast SUSPECT to all members
            down_prot.down(new Event(Event.MSG, suspect_msg));
        }

        public String toString() {
            return FD_SOCK.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }


}
