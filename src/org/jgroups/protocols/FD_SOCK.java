package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.LazyRemovalCache;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.ThreadFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;


/**
 * Failure detection protocol based on sockets. Failure detection is ring-based. Each member creates a
 * server socket and announces its address together with the server socket's address in a multicast.
 * <p>
 * A pinger thread will be started when the membership goes above 1 and will be stopped when it drops below
 * 2. The pinger thread connects to its neighbor on the right and waits until the socket is closed. When
 * the socket is closed by the monitored peer in an abnormal fashion (IOException), the neighbor will be
 * suspected.
 * <p>
 * The main feature of this protocol is that no ping messages need to be exchanged between any 2 peers, as failure
 * detection relies entirely on TCP sockets. The advantage is that no activity will take place between 2 peers as long
 * as they are alive (i.e. have their server sockets open). The disadvantage is that hung servers or crashed routers
 * will not cause sockets to be closed, therefore they won't be detected.
 * <p>
 * The costs involved are 2 additional threads: one that monitors the client side of the socket connection
 * (to monitor a peer) and another one that manages the server socket. However, those threads will be idle as long as
 * both peers are running.
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
    protected InetAddress bind_addr;

    @Property(description="Use \"external_addr\" if you have hosts on different networks, behind " +
      "firewalls. On each firewall, set up a port forwarding rule (sometimes called \"virtual server\") to " +
      "the local IP (e.g. 192.168.1.100) of the host then on each host, set \"external_addr\" TCP transport " +
      "parameter to the external (public IP) address of the firewall.",
              systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress external_addr;

    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
              systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int         external_port;

    @Property(description="Timeout for getting socket cache from coordinator")
    protected long        get_cache_timeout=1000;

    @Property(description="Max number of elements in the cache until deleted elements are removed")
    protected int         cache_max_elements=200;

    @Property(description="Max age (in ms) an element marked as removed has to have until it is removed")
    protected long        cache_max_age=10000;

    @Property(description="Interval for broadcasting suspect messages")
    protected long        suspect_msg_interval=5000;

    @Property(description="Number of attempts coordinator is solicited for socket cache until we give up")
    protected int         num_tries=3;

    @Property(description="Start port for server socket. Default value of 0 picks a random port")
    protected int         start_port;

    @Property(description="Start port for client socket. Default value of 0 picks a random port")
    protected int         client_bind_port;

    @Property(description="Number of ports to probe for start_port and client_bind_port")
    protected int         port_range=50;

    @Property(description="Whether to use KEEP_ALIVE on the ping socket or not. Default is true")
    protected boolean     keep_alive=true;

    @Property(description="Max time in millis to wait for ping Socket.connect() to return")
    protected int         sock_conn_timeout=1000;


    /* ---------------------------------------------   JMX      ------------------------------------------------------ */


    protected int num_suspect_events;

    protected final BoundedList<String> suspect_history=new BoundedList<>(20);


    /* --------------------------------------------- Fields ------------------------------------------------------ */


    protected volatile List<Address> members=new ArrayList<>(11); // volatile eliminates the lock

    protected final Set<Address>     suspected_mbrs=new ConcurrentSkipListSet<>();

    protected final List<Address>    pingable_mbrs=Collections.synchronizedList(new ArrayList<>());

    protected volatile boolean srv_sock_sent; // has own socket been broadcast yet ?
    /** Used to rendezvous on GET_CACHE and GET_CACHE_RSP */
    protected final Promise<Map<Address,IpAddress>> get_cache_promise=new Promise<>();
    protected volatile boolean got_cache_from_coord; // was cache already fetched ?
    protected Address local_addr; // our own address
    protected ServerSocket srv_sock; // server socket to which another member connects to monitor me

    protected ServerSocketHandler srv_sock_handler; // accepts new connections on srv_sock
    protected IpAddress srv_sock_addr; // pair of server_socket:port
    protected Address ping_dest; // address of the member we monitor
    protected Socket ping_sock; // socket to the member we monitor
    protected InputStream ping_input; // input stream of the socket to the member we monitor
    @GuardedBy("this")
    protected volatile Thread pinger_thread; // listens on ping_sock, suspects member if socket is closed

    /** Cache of member addresses and their ServerSocket addresses */
    protected LazyRemovalCache<Address,IpAddress> cache;

    protected final Promise<IpAddress> ping_addr_promise=new Promise<>(); // to fetch the ping_addr for ping_dest

    protected final Lock lock=new ReentrantLock(); // for access to ping_sock, ping_input

    protected TimeScheduler timer;
    protected final BroadcastTask bcast_task=new BroadcastTask(); // to transmit SUSPECT message (until view change)
    protected volatile boolean regular_sock_close; // used by interruptPingerThread() when new ping_dest is computed
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
    @ManagedAttribute(description="Whether the node crash detection monitor is running")
    public boolean isNodeCrashMonitorRunning() {return isPingerThreadRunning(); }
    @ManagedAttribute(description="Whether or not to log suspect messages")
    public boolean isLogSuspectedMessages() {
        return log_suspected_msgs;
    }
    public void setLogSuspectedMessages(boolean log_suspected_msgs) {
        this.log_suspected_msgs=log_suspected_msgs;
    }
    @ManagedAttribute(description="The actual client_bind_port")
    public int  getClientBindPortActual() {return ping_sock != null? ping_sock.getLocalPort() : 0;}

    @ManagedOperation(description="Print suspect history")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(String suspect: suspect_history)
            sb.append(suspect).append("\n");
        return sb.toString();
    }

    @ManagedOperation
    public String printCache() {
        return cache.printCache();
    }

    /*@ManagedOperation
    public void clearCache() {
        cache.clear(false);
    }*/

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
        cache=new LazyRemovalCache<>(cache_max_elements, cache_max_age);
        shuttin_down=false;
        srv_sock_handler=new ServerSocketHandler();
    }


    public void start() throws Exception {
        shuttin_down=false;
        super.start();
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
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
            case Event.CONFIG:
                Map<String,Object> config=evt.getArg();
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
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        FdHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);  // message did not originate from FD_SOCK layer, just pass up

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
                    hdr.mbrs.forEach(this::unsuspect);
                }
                break;

            // If I have the sock for 'hdr.mbr', return it. Otherwise look it up in my cache and return it
            case FdHeader.WHO_HAS_SOCK:
                if(Objects.equals(local_addr, msg.getSrc()))
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
                cache.add(hdr.mbr, hdr.sock_addr); // update the cache
                log.trace("%s: i-have-sock: %s --> %s (cache is %s)", local_addr, hdr.mbr, hdr.sock_addr, cache);

                if(hdr.mbr.equals(ping_dest))
                    ping_addr_promise.setResult(hdr.sock_addr);
                break;

            // Return the cache to the sender of this message
            case FdHeader.GET_CACHE:
                msg=new Message(msg.getSrc()).setFlag(Message.Flag.INTERNAL)
                  .putHeader(this.id, new FdHeader(FdHeader.GET_CACHE_RSP)).setBuffer(marshal(cache));
                down_prot.down(msg);
                break;

            case FdHeader.GET_CACHE_RSP:
                Map<Address,IpAddress> cachedAddrs=unmarshal(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                if(cachedAddrs != null)
                    get_cache_promise.setResult(cachedAddrs);
                break;
        }
        return null;
    }


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.UNSUSPECT:
                broadcastUnuspectMessage(evt.getArg());
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
                local_addr=evt.getArg();
                break;

            case Event.VIEW_CHANGE:
                View v=evt.getArg();
                final List<Address> new_mbrs=v.getMembers();

                members=new_mbrs;  // volatile write will ensure all reads after this see the new membership
                suspected_mbrs.retainAll(new_mbrs);
                cache.keySet().retainAll(new_mbrs); // remove all entries in 'cache' which are not in the new membership
                bcast_task.adjustSuspectedMembers(new_mbrs);
                pingable_mbrs.clear();
                pingable_mbrs.addAll(new_mbrs);
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
        if(!srv_sock_sent && srv_sock_addr != null) {
            sendIHaveSockMessage(null, local_addr, srv_sock_addr);
            srv_sock_sent=true;
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
            if(ping_dest == null || !isPingerThreadRunning())
                break;
            log.debug("%s: pingable_mbrs=%s, ping_dest=%s", local_addr, pingable_mbrs, ping_dest);

            IpAddress ping_addr=fetchPingAddress(ping_dest);
            if(ping_addr == null) {
                log.trace("%s: socket address for %s could not be fetched, retrying", local_addr, ping_dest);
                Util.sleep(1000);
                continue;
            }

            if(!setupPingSocket(ping_addr) && isPingerThreadRunning()) {
                // log.debug("%s: failed connecting to to %s", local_addr, ping_dest);
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
                            log.debug("%s: %s closed socket gracefully", local_addr, ping_dest);
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
        suspects.forEach(suspect -> suspect_history.add(String.format("%s: %s", new Date(), suspect)));

        suspected_mbrs.addAll(suspects);
        List<Address> eligible_mbrs=new ArrayList<>(this.members);
        eligible_mbrs.removeAll(suspected_mbrs);

        // Check if we're coord, then send up the stack
        if(local_addr != null && !eligible_mbrs.isEmpty() && local_addr.equals(eligible_mbrs.get(0))) {
            log.debug("%s: suspecting %s", local_addr, suspected_mbrs);
            up_prot.up(new Event(Event.SUSPECT, suspected_mbrs));
            down_prot.down(new Event(Event.SUSPECT, suspected_mbrs));
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
            log.debug("%s: %s closed socket (%s)", local_addr, ping_dest, (ex != null ? ex.toString() : "eof"));
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
        lock.lock();
        try {
            if(ping_sock != null) {
                OutputStream out=ping_sock.getOutputStream();
                out.write(signal);
                out.flush();
            }
        }
        catch(Throwable t) {
            log.trace("%s: problem sending signal %s: %s", local_addr, signalToString(signal), t);
        }
        finally {
            lock.unlock();
        }
    }




    protected void startServerSocket() throws Exception {
        srv_sock=Util.createServerSocket(getSocketFactory(),
                                         "jgroups.fd_sock.srv_sock", bind_addr, start_port, start_port+port_range); // grab a random unused port above 10000
        srv_sock_addr=new IpAddress(external_addr != null? external_addr : bind_addr, external_port > 0? external_port : srv_sock.getLocalPort());
        if(local_addr != null)
            cache.add(local_addr, srv_sock_addr);
        if(srv_sock_handler != null)
            srv_sock_handler.start(); // won't start if already running
    }

    public void stopServerSocket(boolean graceful) {
        if(srv_sock_handler != null)
            srv_sock_handler.stop(graceful);
    }


    /**
     * Creates a socket to {@code dest}, and assigns it to ping_sock. Also assigns ping_input
     */
    protected boolean setupPingSocket(IpAddress dest) {
        lock.lock();
        try {
            SocketAddress destAddr=new InetSocketAddress(dest.getIpAddress(), dest.getPort());
            ping_sock=getSocketFactory().createSocket("jgroups.fd.ping_sock");
            Util.bind(ping_sock, bind_addr, client_bind_port, client_bind_port+port_range);
            ping_sock.setSoLinger(true, 1);
            ping_sock.setKeepAlive(keep_alive);
            Util.connect(ping_sock, destAddr, sock_conn_timeout);
            ping_input=ping_sock.getInputStream();
            return true;
        }
        catch(Throwable ex) {
            if(!shuttin_down)
                log.debug("%s: failed connecting to %s: %s",
                          local_addr, ping_dest != null? ping_dest : dest, ex.getMessage());
            return false;
        }
        finally {
            lock.unlock();
        }
    }


    protected void teardownPingSocket() {
        lock.lock();
        try {
            if(ping_sock != null) {
                try {
                    ping_sock.shutdownInput();
                    ping_sock.close();
                }
                catch(Exception ex) {
                }
            }
            Util.close(ping_input);
        }
        finally {
            ping_sock=null;
            ping_input=null;
            lock.unlock();
        }
    }


    /**
     * Determines coordinator C. If C is null and we are the first member, return. Else loop: send GET_CACHE message
     * to coordinator and wait for GET_CACHE_RSP response. Loop until valid response has been received.
     */
    protected void getCacheFromCoordinator() {
        Address coord;
        int attempts=num_tries;

        get_cache_promise.reset();
        while(attempts > 0 && isPingerThreadRunning()) {
            if((coord=determineCoordinator()) != null) {
                if(coord.equals(local_addr)) { // we are the first member --> empty cache
                    return;
                }
                Message msg=new Message(coord).setFlag(Message.Flag.INTERNAL)
                  .putHeader(this.id, new FdHeader(FdHeader.GET_CACHE));
                down_prot.down(msg);
                Map<Address,IpAddress> result=get_cache_promise.getResult(get_cache_timeout);
                if(result != null) {
                    cache.addAll(result);
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

        log.debug("%s: broadcasting suspect(%s)", local_addr, suspected_mbr);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        FdHeader hdr=new FdHeader(FdHeader.SUSPECT).mbrs(Collections.singleton(suspected_mbr));
        Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(this.id, hdr);
        down_prot.down(suspect_msg);

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

        log.debug("%s: broadcasting unsuspect(%s)", local_addr, mbr);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        FdHeader hdr=new FdHeader(FdHeader.UNSUSPECT).mbrs(Collections.singleton(mbr));
        Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(this.id, hdr);
        down_prot.down(suspect_msg);
    }



    /**
     Sends or broadcasts a I_HAVE_SOCK response. If 'dst' is null, the reponse will be broadcast, otherwise
     it will be unicast back to the requester
     */
    protected void sendIHaveSockMessage(Address dst, Address mbr, IpAddress addr) {
        Message msg=new Message(dst).setFlag(Message.Flag.INTERNAL).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
        FdHeader hdr=new FdHeader(FdHeader.I_HAVE_SOCK, mbr);
        hdr.sock_addr=addr;
        msg.putHeader(this.id, hdr);
        down_prot.down(msg);
    }


    /**
     Attempts to obtain the ping_addr first from the cache, then by unicasting q request to {@code mbr},
     then by multicasting a request to all members.
     */
    protected IpAddress fetchPingAddress(final Address mbr) {
        IpAddress ret;

        if(mbr == null)
            return null;

        // 1. Try to get the server socket address from the cache
        if((ret=cache.get(mbr)) != null)
            return ret;

        if(!isPingerThreadRunning()) return null;

        // 2. Try to get the server socket address from mbr (or all, as fallback)
        ping_addr_promise.reset();
        for(Address dest: Arrays.asList(mbr, null)) {
            Message msg=new Message(dest).setFlag(Message.Flag.INTERNAL)
              .putHeader(this.id, new FdHeader(FdHeader.WHO_HAS_SOCK, mbr));
            down_prot.down(msg);
            if((ret=ping_addr_promise.getResult(500)) != null)
                return ret;
            if(!isPingerThreadRunning()) return null;
        }
        return null;
    }


    protected Address determinePingDest() {
        if(pingable_mbrs == null || local_addr == null)
            return null;
        Address next=Util.pickNext(pingable_mbrs, local_addr);
        return Objects.equals(local_addr, next) ? null : next;
    }

    public static Buffer marshal(LazyRemovalCache<Address,IpAddress> addrs) {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        try {
            int size=addrs != null? addrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Map.Entry<Address,LazyRemovalCache.Entry<IpAddress>> entry: addrs.entrySet()) {
                    Address key=entry.getKey();
                    IpAddress val=entry.getValue().getVal();
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

    protected Map<Address,IpAddress> unmarshal(byte[] buffer, int offset, int length) {
        if(buffer == null) return null;
        DataInput in=new ByteArrayDataInputStream(buffer, offset, length);
        HashMap<Address,IpAddress> addrs=null;
        try {
            int size=in.readInt();
            if(size > 0) {
                addrs=new HashMap<>(size);
                for(int i=0; i < size; i++) {
                    Address key=Util.readAddress(in);
                    IpAddress val=Util.readStreamable(IpAddress::new, in);
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


        protected byte           type=SUSPECT;
        protected Address        mbr;           // set on WHO_HAS_SOCK (requested mbr), I_HAVE_SOCK
        protected IpAddress      sock_addr;     // set on I_HAVE_SOCK
        protected Set<Address>   mbrs;          // set on SUSPECT (list of suspected members)


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


        public short getMagicId() {
            return 51;
        }

        public Supplier<? extends Header> create() {
            return FdHeader::new;
        }

        public FdHeader mbrs(Set<Address> members) {
            this.mbrs=members; return this;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(type2String(type));
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

        @Override
        public int serializedSize() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Util.size(mbr);

            // use of Util.size(Address) with IpAddress overestimates size by one byte.
            // replace: retval+=Util.size(sock_addr); with the following:
            int ipaddr_size = 0 ;
            ipaddr_size += Global.BYTE_SIZE ;     // presence byte
            if (sock_addr != null)
              ipaddr_size += sock_addr.serializedSize();  // IpAddress size
            retval += ipaddr_size ;
            retval+=Global.INT_SIZE; // mbrs size
            if(mbrs != null)
                for(Address m: mbrs)
                    retval+=Util.size(m);
            return retval;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            Util.writeAddress(mbr, out);
            Util.writeStreamable(sock_addr, out);
            int size=mbrs != null? mbrs.size() : 0;
            out.writeInt(size);
            if(size > 0)
                for(Address address: mbrs)
                    Util.writeAddress(address, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            mbr=Util.readAddress(in);
            sock_addr=Util.readStreamable(IpAddress::new, in);
            int size=in.readInt();
            if(size > 0) {
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
        protected Thread                              acceptor;
        protected final List<ClientConnectionHandler> clients=new LinkedList<>();


        protected String getName() {
            return acceptor != null? acceptor.getName() : null;
        }

        protected ServerSocketHandler() {
            start();
        }

        protected void start() {
            if(acceptor == null) {
                acceptor=getThreadFactory().newThread(this, "FD_SOCK acceptor");
                acceptor.setDaemon(true);
                acceptor.start();
            }
        }


        protected void stop(boolean graceful) {
            if(acceptor != null && acceptor.isAlive())
                Util.close(srv_sock); // this will terminate thread, peer will receive SocketException (socket close)

            synchronized(clients) {
                clients.forEach(client -> client.stopThread(graceful));
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
                    ThreadFactory factory=getThreadFactory();
                    Thread t = factory != null? factory.newThread(client_conn_handler, "FD_SOCK conn-handler")
                      : new Thread(client_conn_handler, "FD_SOCK conn-handler");
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
        Socket                              client_sock;
        InputStream                         in;
        final List<ClientConnectionHandler> clients;

        protected ClientConnectionHandler(Socket client_sock, List<ClientConnectionHandler> clients) {
            this.client_sock=client_sock;
            this.clients=clients;
        }

        protected synchronized void stopThread(boolean graceful) {
            if(client_sock != null) {
                try {
                    if(graceful) {
                        OutputStream out=client_sock.getOutputStream();
                        out.write(NORMAL_TERMINATION);
                        out.flush();
                    }
                    Util.close(client_sock);
                    client_sock=null;
                }
                catch(Throwable t) {
                }
            }
        }


        public void run() {
            try {
                synchronized(this) {
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
                if (sock != null && !sock.isClosed()) {
                    Util.close(sock);
                    client_sock=null;
                }
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
        protected final Set<Address>    suspects=new HashSet<>();
        protected Future<?>             future;


        /** Adds a suspected member. Starts the task if not yet running */
        protected void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            if(!members.contains(mbr)) return;
            synchronized(suspects) {
                if(suspects.add(mbr))
                    startTask();
            }
        }


        protected void removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return;
            synchronized(suspects) {
                if(suspects.remove(suspected_mbr) && suspects.isEmpty())
                    stopTask();
            }
        }


        protected void removeAll() {
            synchronized(suspects) {
                suspects.clear();
                stopTask();
            }
        }


        protected void startTask() {
            if(future == null || future.isDone()) {
                try {
                    future=timer.scheduleWithFixedDelay(this, suspect_msg_interval, suspect_msg_interval, TimeUnit.MILLISECONDS,
                                                        getTransport() instanceof TCP);
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
        protected void adjustSuspectedMembers(List<Address> new_mbrship) {
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
                hdr=new FdHeader(FdHeader.SUSPECT).mbrs(new HashSet<>(suspects));
            }
            Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(id, hdr); // mcast SUSPECT to all members
            down_prot.down(suspect_msg);
        }

        public String toString() {
            return FD_SOCK.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }


}
