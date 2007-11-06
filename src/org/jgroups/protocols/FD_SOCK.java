
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

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
 * @version $Id: FD_SOCK.java,v 1.83 2007/11/06 17:30:11 vlada Exp $
 */
public class FD_SOCK extends Protocol implements Runnable {
    long                        get_cache_timeout=1000;            // msecs to wait for the socket cache from the coordinator
    long                        suspect_msg_interval=5000;         // (BroadcastTask): mcast SUSPECT every 5000 msecs
    int                         num_tries=3;                       // attempts coord is solicited for socket cache until we give up
    final Vector<Address>       members=new Vector<Address>(11);            // list of group members (updated on VIEW_CHANGE)
    boolean                     srv_sock_sent=false;               // has own socket been broadcast yet ?
    final Vector<Address>       pingable_mbrs=new Vector<Address>(11);      // mbrs from which we select ping_dest. may be subset of 'members'
    /** Used to rendezvous on GET_CACHE and GET_CACHE_RSP */
    final Promise<Map<Address,IpAddress>>  get_cache_promise=new Promise<Map<Address,IpAddress>>();
    boolean                     got_cache_from_coord=false;        // was cache already fetched ?
    Address                     local_addr=null;                   // our own address
    ServerSocket                srv_sock=null;                     // server socket to which another member connects to monitor me

    InetAddress                 bind_addr=null;                    // the NIC on which the ServerSocket should listen

    String                      group_name=null;                   // the name of the group (set on CONNECT, nulled on DISCONNECT)

    private ServerSocketHandler srv_sock_handler=null;             // accepts new connections on srv_sock
    IpAddress                   srv_sock_addr=null;                // pair of server_socket:port
    Address                     ping_dest=null;                    // address of the member we monitor
    Socket                      ping_sock=null;                    // socket to the member we monitor
    InputStream                 ping_input=null;                   // input stream of the socket to the member we monitor
    @GuardedBy("this")
    volatile Thread             pinger_thread=null;                // listens on ping_sock, suspects member if socket is closed

    /** Cache of member addresses and their ServerSocket addresses */
    final ConcurrentMap<Address,IpAddress> cache=new ConcurrentHashMap<Address,IpAddress>(11);

    /** Start port for server socket (uses first available port starting at start_port). A value of 0 (default)
     * picks a random port */
    int                         start_port=0;
    final Promise<IpAddress>    ping_addr_promise=new Promise<IpAddress>();   // to fetch the ping_addr for ping_dest
    final Object                sock_mutex=new Object();           // for access to ping_sock, ping_input
    TimeScheduler               timer=null;
    private final BroadcastTask bcast_task=new BroadcastTask();    // to transmit SUSPECT message (until view change)
    boolean                     regular_sock_close=false;         // used by interruptPingerThread() when new ping_dest is computed
    int                         num_suspect_events=0;
    private static final int    INTERRUPT =8;
    private static final int    NORMAL_TERMINATION=9;
    private static final int    ABNORMAL_TERMINATION=-1;
    private static final String name="FD_SOCK";

    final BoundedList<Address>  suspect_history=new BoundedList<Address>(20);

    /** whether to use KEEP_ALIVE on the ping socket or not */
    private boolean             keep_alive=true;

    private volatile boolean    running=false;


    public String getName() {
        return name;
    }

    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "null";}
    public String getPingableMembers() {return pingable_mbrs != null? pingable_mbrs.toString() : "null";}
    public String getPingDest() {return ping_dest != null? ping_dest.toString() : "null";}
    public int getNumSuspectEventsGenerated() {return num_suspect_events;}
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Address suspect: suspect_history) {
            sb.append(new Date()).append(": ").append(suspect).append("\n");
        }
        return sb.toString();
    }

    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("get_cache_timeout");
        if(str != null) {
            get_cache_timeout=Long.parseLong(str);
            props.remove("get_cache_timeout");
        }

        str=props.getProperty("suspect_msg_interval");
        if(str != null) {
            suspect_msg_interval=Long.parseLong(str);
            props.remove("suspect_msg_interval");
        }

        str=props.getProperty("num_tries");
        if(str != null) {
            num_tries=Integer.parseInt(str);
            props.remove("num_tries");
        }

        str=props.getProperty("start_port");
        if(str != null) {
            start_port=Integer.parseInt(str);
            props.remove("start_port");
        }

        str=props.getProperty("keep_alive");
        if(str != null) {
            keep_alive=Boolean.parseBoolean(str);
            props.remove("keep_alive");
        }

        str=props.getProperty("srv_sock_bind_addr");
        if(str != null) {
            log.error("srv_sock_bind_addr is deprecated and will be ignored - use bind_addr instead");
            props.remove("srv_sock_bind_addr");
        }

        try {
            bind_addr=Util.getBindAddress(props);
        }
        catch(UnknownHostException unknown) {
            log.fatal("failed getting bind_addr", unknown);
            return false;
        }
        catch(SocketException ex) {
            log.fatal("failed getting bind_addr", ex);
            return false;
        }

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public String printCache() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,IpAddress> entry: cache.entrySet()) {
            sb.append(entry.getKey()).append(" has server socket at ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    public void init() throws Exception {
        srv_sock_handler=new ServerSocketHandler();
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("FD_SOCK.init(): timer == null");
    }


    public void start() throws Exception {
        super.start();
        running=true;
    }

    public void stop() {
        running=false;
        bcast_task.removeAll();
        synchronized(this) {
            stopPingerThread();
        }
        stopServerSocket(true); // graceful close
    }

    public void resetStats() {
        super.resetStats();
        num_suspect_events=0;
        suspect_history.clear();
    }


    public Object up(Event evt) {
        switch(evt.getType()) {

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address) evt.getArg();
            break;
                           
        case Event.MSG:
            Message msg=(Message) evt.getArg();
            FdHeader hdr=(FdHeader)msg.getHeader(name);
            if(hdr == null)
                break;  // message did not originate from FD_SOCK layer, just pass up

            switch(hdr.type) {

            case FdHeader.SUSPECT:
                if(hdr.mbrs != null) {
                    if(log.isDebugEnabled()) log.debug("[SUSPECT] hdr=" + hdr);
                    for(Address m: hdr.mbrs) {
                        if(local_addr != null && m.equals(local_addr)) {
                            if(log.isWarnEnabled())
                                log.warn("I was suspected by " + msg.getSrc() + "; ignoring the SUSPECT message");
                            continue;
                        }
                        up_prot.up(new Event(Event.SUSPECT, m));
                        down_prot.down(new Event(Event.SUSPECT, m));
                    }
                }
                else
                    if(log.isWarnEnabled()) log.warn("[SUSPECT]: hdr.mbrs == null");
                break;

                // If I have the sock for 'hdr.mbr', return it. Otherwise look it up in my cache and return it
            case FdHeader.WHO_HAS_SOCK:
                if(local_addr != null && local_addr.equals(msg.getSrc()))
                    return null; // don't reply to WHO_HAS bcasts sent by me !

                if(hdr.mbr == null) {
                    if(log.isErrorEnabled()) log.error("hdr.mbr is null");
                    return null;
                }

                if(log.isTraceEnabled()) log.trace("who-has-sock " + hdr.mbr);

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
                if(hdr.mbr == null || hdr.sock_addr == null) {
                    if(log.isErrorEnabled()) log.error("[I_HAVE_SOCK]: hdr.mbr is null or hdr.sock_addr == null");
                    return null;
                }

                // if(!cache.containsKey(hdr.mbr))
                cache.put(hdr.mbr, hdr.sock_addr); // update the cache
                if(log.isTraceEnabled()) log.trace("i-have-sock: " + hdr.mbr + " --> " +
                                                   hdr.sock_addr + " (cache is " + cache + ')');

                if(ping_dest != null && hdr.mbr.equals(ping_dest))
                    ping_addr_promise.setResult(hdr.sock_addr);
                break;

                // Return the cache to the sender of this message
            case FdHeader.GET_CACHE:
                Address sender=msg.getSrc(); // guaranteed to be non-null
                hdr=new FdHeader(FdHeader.GET_CACHE_RSP,new HashMap<Address,IpAddress>(cache));                
                msg=new Message(sender, null, null);
                msg.setFlag(Message.OOB);
                msg.putHeader(name, hdr);
                down_prot.down(new Event(Event.MSG, msg));
                break;

            case FdHeader.GET_CACHE_RSP:
                if(hdr.cachedAddrs == null) {
                    if(log.isWarnEnabled()) log.warn("(GET_CACHE_RSP): cache is null");
                    return null;
                }
                get_cache_promise.setResult(hdr.cachedAddrs);
                break;
            }
            return null;

            case Event.CONFIG:
                if(bind_addr == null) {
                    Map<String,Object> config=(Map<String,Object>)evt.getArg();
                    bind_addr=(InetAddress)config.get("bind_addr");
                }
                break;
        }

        return up_prot.up(evt);                                        // pass up to the layer above us
    }


    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.UNSUSPECT:
                bcast_task.removeSuspectedMember((Address)evt.getArg());
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:    
                Object ret=down_prot.down(evt);
                group_name=(String)evt.getArg();
                srv_sock=Util.createServerSocket(bind_addr, start_port); // grab a random unused port above 10000
                srv_sock_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
                startServerSocket();
                return ret;

            case Event.DISCONNECT:                
                stopServerSocket(true); // graceful close
                break;

            case Event.SHUTDOWN:
                stopServerSocket(false);
                break;

            case Event.VIEW_CHANGE:
                View v=(View) evt.getArg();
                final Vector<Address> new_mbrs=v.getMembers();

                Runnable reshuffleSockets=new Runnable() {
                    public void run() {
                        synchronized(FD_SOCK.this) {
                            members.removeAllElements();
                            members.addAll(new_mbrs);
                            cache.keySet().retainAll(members); // remove all entries in 'cache' which are not in the new membership
                            bcast_task.adjustSuspectedMembers(members);
                            pingable_mbrs.removeAllElements();
                            pingable_mbrs.addAll(members);
                            if(log.isDebugEnabled()) log.debug("VIEW_CHANGE received: " + members);

                            if(members.size() > 1) {
                                if(pinger_thread != null && pinger_thread.isAlive()) {
                                    Address tmp_ping_dest=determinePingDest();
                                    if(ping_dest != null && tmp_ping_dest != null && !ping_dest.equals(tmp_ping_dest)) {
                                        interruptPingerThread(); // allows the thread to use the new socket
                                    }
                                }
                                else
                                    startPingerThread(); // only starts if not yet running
                            }
                            else {
                                ping_dest=null;
                                stopPingerThread();
                            }
                        }
                    }
                };
                timer.submit(reshuffleSockets);
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
        Address tmp_ping_dest;
        IpAddress ping_addr;

        // 1. Broadcast my own addr:sock to all members so they can update their cache
        if(!srv_sock_sent) {
            if(srv_sock_addr != null) {
                sendIHaveSockMessage(null, // send to all members
                                     local_addr,
                                     srv_sock_addr);
                srv_sock_sent=true;
            }
            else
            if(log.isWarnEnabled()) log.warn("(VIEW_CHANGE): srv_sock_addr == null");
        }

        // 2. Get the addr:pid cache from the coordinator (only if not already fetched)
        if(!got_cache_from_coord) {
            getCacheFromCoordinator();
            got_cache_from_coord=true;
        }



        if(log.isTraceEnabled()) log.trace("pinger_thread started"); // +++ remove
        while(pinger_thread != null && Thread.currentThread().equals(pinger_thread) && running) {
            tmp_ping_dest=determinePingDest(); // gets the neighbor to our right
            if(log.isDebugEnabled())
                log.debug("determinePingDest()=" + tmp_ping_dest + ", pingable_mbrs=" + pingable_mbrs);
            if(tmp_ping_dest == null) {
                ping_dest=null;
                synchronized(this) {
                    pinger_thread=null;
                }
                break;
            }
            ping_dest=tmp_ping_dest;
            ping_addr=fetchPingAddress(ping_dest);
            if(ping_addr == null) {
                if(!running)
                    break;
                if(log.isErrorEnabled()) log.error("socket address for " + ping_dest + " could not be fetched, retrying");
                Util.sleep(1000);
                continue;
            }

            if(!setupPingSocket(ping_addr)) {
                // covers use cases #7 and #8 in ManualTests.txt
                if(log.isDebugEnabled()) log.debug("could not create socket to " + ping_dest + "; suspecting " + ping_dest);
                broadcastSuspectMessage(ping_dest);
                pingable_mbrs.removeElement(ping_dest);
                continue;
            }

            if(log.isDebugEnabled()) log.debug("ping_dest=" + ping_dest + ", ping_sock=" + ping_sock + ", cache=" + cache);

            // at this point ping_input must be non-null, otherwise setupPingSocket() would have thrown an exception
            try {
                if(ping_input != null) {
                    int c=ping_input.read();
                    switch(c) {
                        case NORMAL_TERMINATION:
                            if(log.isDebugEnabled())
                                log.debug("peer closed socket normally");
                            synchronized(this) {
                                pinger_thread=null;
                            }
                            break;
                        case ABNORMAL_TERMINATION:
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
        if(log.isDebugEnabled()) log.debug("pinger thread terminated");
        synchronized(this) {
            pinger_thread=null;
        }
    }




    /* ----------------------------------- Private Methods -------------------------------------- */


    void handleSocketClose(Exception ex) {
        teardownPingSocket();     // make sure we have no leftovers
        if(!regular_sock_close) { // only suspect if socket was not closed regularly (by interruptPingerThread())
            if(log.isDebugEnabled())
                log.debug("peer " + ping_dest + " closed socket (" + (ex != null ? ex.getClass().getName() : "eof") + ')');
            broadcastSuspectMessage(ping_dest);
            pingable_mbrs.removeElement(ping_dest);
        }
        else {
            if(log.isDebugEnabled()) log.debug("socket to " + ping_dest + " was reset");
            regular_sock_close=false;
        }
    }


    /**
     * Does *not* need to be synchronized on pinger_mutex because the caller (down()) already has the mutex acquired
     */
    void startPingerThread() {
        running=true;
        if(pinger_thread == null) {
            pinger_thread=getProtocolStack().getThreadFactory().newThread(this, "FD_SOCK pinger");            
            pinger_thread.setDaemon(true);            
            pinger_thread.start();            
        }
    }


    void stopPingerThread() {
        running=false;
        if(pinger_thread != null && pinger_thread.isAlive()) {
            regular_sock_close=true;
            pinger_thread=null;
            sendPingTermination(); // PATCH by Bruce Schuchardt (http://jira.jboss.com/jira/browse/JGRP-246)
            teardownPingSocket();
            ping_addr_promise.setResult(null);
            get_cache_promise.setResult(null);
        }

    }

    // PATCH: send something so the connection handler can exit
    void sendPingTermination() {
        sendPingSignal(NORMAL_TERMINATION);
    }

    void sendPingInterrupt() {
        sendPingSignal(INTERRUPT);
    }


    void sendPingSignal(int signal) {
        if(ping_sock != null) {
            try {
                OutputStream out=ping_sock.getOutputStream();
                if(out != null) {
                    out.write(signal);
                    out.flush();
                }
            }
            catch(Throwable t) {
                if(log.isTraceEnabled())
                    log.trace("problem sending signal " + signalToString(signal), t);
            }
        }
    }




    /**
     * Interrupts the pinger thread. The Thread.interrupt() method doesn't seem to work under Linux with JDK 1.3.1
     * (JDK 1.2.2 had no problems here), therefore we close the socket (setSoLinger has to be set !) if we are
     * running under Linux. This should be tested under Windows. (Solaris 8 and JDK 1.3.1 definitely works).<p>
     * Oct 29 2001 (bela): completely removed Thread.interrupt(), but used socket close on all OSs. This makes this
     * code portable and we don't have to check for OSs.<p/>
     * Does *not* need to be synchronized on pinger_mutex because the caller (down()) already has the mutex acquired
     * @see {@link org.jgroups.tests.InterruptTest} to determine whether Thread.interrupt() works for InputStream.read().
     */
    void interruptPingerThread() {
        if(pinger_thread != null && pinger_thread.isAlive()) {
            regular_sock_close=true;
            sendPingInterrupt();  // PATCH by Bruce Schuchardt (http://jira.jboss.com/jira/browse/JGRP-246)
            teardownPingSocket(); // will wake up the pinger thread. less elegant than Thread.interrupt(), but does the job
        }
    }

    void startServerSocket() {
        if(srv_sock_handler != null) {
            srv_sock_handler.start(); // won't start if already running            
        }
    }

    void stopServerSocket(boolean graceful) {
        if(srv_sock_handler != null)
            srv_sock_handler.stop(graceful);
    }


    /**
     * Creates a socket to <code>dest</code>, and assigns it to ping_sock. Also assigns ping_input
     */
    boolean setupPingSocket(IpAddress dest) {
        synchronized(sock_mutex) {
            if(dest == null) {
                if(log.isErrorEnabled()) log.error("destination address is null");
                return false;
            }
            try {
                ping_sock=new Socket(dest.getIpAddress(), dest.getPort());
                ping_sock.setSoLinger(true, 1);
                ping_sock.setKeepAlive(keep_alive);
                ping_input=ping_sock.getInputStream();
                return true;
            }
            catch(Throwable ex) {
                return false;
            }
        }
    }


    void teardownPingSocket() {
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
    void getCacheFromCoordinator() {
        Address coord;
        int attempts=num_tries;
        Message msg;
        FdHeader hdr;
        Map<Address,IpAddress> result;

        get_cache_promise.reset();
        while(attempts > 0 && running) {
            if((coord=determineCoordinator()) != null) {
                if(coord.equals(local_addr)) { // we are the first member --> empty cache
                    if(log.isDebugEnabled()) log.debug("first member; cache is empty");
                    return;
                }
                hdr=new FdHeader(FdHeader.GET_CACHE);
                msg=new Message(coord, null, null);
                msg.setFlag(Message.OOB);
                msg.putHeader(name, hdr);
                down_prot.down(new Event(Event.MSG, msg));
                result=get_cache_promise.getResult(get_cache_timeout);
                if(result != null) {
                    cache.putAll(result);
                    if(log.isTraceEnabled()) log.trace("got cache from " + coord + ": cache is " + cache);
                    return;
                }
                else {
                    if(log.isWarnEnabled()) log.warn("received null cache; retrying");
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
    void broadcastSuspectMessage(Address suspected_mbr) {
        Message suspect_msg;
        FdHeader hdr;

        if(suspected_mbr == null) return;

        if(log.isTraceEnabled()) log.trace("suspecting " + suspected_mbr + " (own address is " + local_addr + ')');

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        hdr=new FdHeader(FdHeader.SUSPECT);
        hdr.mbrs=new HashSet<Address>(1);
        hdr.mbrs.add(suspected_mbr);
        suspect_msg=new Message();
        suspect_msg.setFlag(Message.OOB);
        suspect_msg.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, suspect_msg));

        // 2. Add to broadcast task and start latter (if not yet running). The task will end when
        //    suspected members are removed from the membership
        bcast_task.addSuspectedMember(suspected_mbr);
        if(stats) {
            num_suspect_events++;
            suspect_history.add(suspected_mbr);
        }
    }




    /**
     Sends or broadcasts a I_HAVE_SOCK response. If 'dst' is null, the reponse will be broadcast, otherwise
     it will be unicast back to the requester
     */
    void sendIHaveSockMessage(Address dst, Address mbr, IpAddress addr) {
        Message msg=new Message(dst, null, null);
        msg.setFlag(Message.OOB);
        FdHeader hdr=new FdHeader(FdHeader.I_HAVE_SOCK);
        hdr.mbr=mbr;
        hdr.sock_addr=addr;
        msg.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }


    /**
     Attempts to obtain the ping_addr first from the cache, then by unicasting q request to <code>mbr</code>,
     then by multicasting a request to all members.
     */
    private IpAddress fetchPingAddress(Address mbr) {
        IpAddress ret;
        Message ping_addr_req;
        FdHeader hdr;

        if(mbr == null) {
            if(log.isErrorEnabled()) log.error("mbr == null");
            return null;
        }
        // 1. Try to get the server socket address from the cache
        if((ret=cache.get(mbr)) != null)
            return ret;

        // 2. Try to get the server socket address from mbr
        ping_addr_promise.reset();
        ping_addr_req=new Message(mbr, null, null); // unicast
        ping_addr_req.setFlag(Message.OOB);
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        ping_addr_req.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, ping_addr_req));
        if(!running) return null;
        ret=ping_addr_promise.getResult(500);
        if(ret != null) {
            return ret;
        }

        // 3. Try to get the server socket address from all members
        ping_addr_req=new Message(null); // multicast
        ping_addr_req.setFlag(Message.OOB);
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        ping_addr_req.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, ping_addr_req));
        ret=ping_addr_promise.getResult(500);
        return ret;
    }


    private Address determinePingDest() {
        Address tmp;

        if(pingable_mbrs == null || pingable_mbrs.size() < 2 || local_addr == null)
            return null;
        for(int i=0; i < pingable_mbrs.size(); i++) {
            tmp=pingable_mbrs.elementAt(i);
            if(local_addr.equals(tmp)) {
                if(i + 1 >= pingable_mbrs.size())
                    return pingable_mbrs.elementAt(0);
                else
                    return pingable_mbrs.elementAt(i + 1);
            }
        }
        return null;
    }


    Address determineCoordinator() {
        return !members.isEmpty()? members.elementAt(0) : null;
    }


    static String signalToString(int signal) {
        switch(signal) {
            case NORMAL_TERMINATION: return "NORMAL_TERMINATION";
            case ABNORMAL_TERMINATION: return "ABNORMAL_TERMINATION";
            case INTERRUPT: return "INTERRUPT";
            default: return "n/a";
        }
    }




    /* ------------------------------- End of Private Methods ------------------------------------ */


    public static class FdHeader extends Header implements Streamable {
        public static final byte SUSPECT=10;
        public static final byte WHO_HAS_SOCK=11;
        public static final byte I_HAVE_SOCK=12;
        public static final byte GET_CACHE=13; // sent by joining member to coordinator
        public static final byte GET_CACHE_RSP=14; // sent by coordinator to joining member in response to GET_CACHE


        byte                      type=SUSPECT;
        Address                   mbr=null;           // set on WHO_HAS_SOCK (requested mbr), I_HAVE_SOCK
        IpAddress                 sock_addr;          // set on I_HAVE_SOCK
        Map<Address,IpAddress>    cachedAddrs=null;   // set on GET_CACHE_RSP
        Set<Address>              mbrs=null;          // set on SUSPECT (list of suspected members)
        private static final long serialVersionUID=-7025890133989522764L;


        public FdHeader() {
        } // used for externalization

        public FdHeader(byte type) {
            this.type=type;
        }

        public FdHeader(byte type, Address mbr) {
            this.type=type;
            this.mbr=mbr;
        }

        public FdHeader(byte type, Set<Address> mbrs) {
            this.type=type;
            this.mbrs=mbrs;
        }

        public FdHeader(byte type, Map<Address,IpAddress> cachedAddrs) {
            this.type=type;
            this.cachedAddrs=cachedAddrs;
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type2String(type));
            if(mbr != null)
                sb.append(", mbr=").append(mbr);
            if(sock_addr != null)
                sb.append(", sock_addr=").append(sock_addr);
            if(cachedAddrs != null)
                sb.append(", cache=").append(cachedAddrs);
            if(mbrs != null)
                sb.append(", mbrs=").append(mbrs);
            return sb.toString();
        }


        public static String type2String(byte type) {
            switch(type) {
                case SUSPECT:
                    return "SUSPECT";
                case WHO_HAS_SOCK:
                    return "WHO_HAS_SOCK";
                case I_HAVE_SOCK:
                    return "I_HAVE_SOCK";
                case GET_CACHE:
                    return "GET_CACHE";
                case GET_CACHE_RSP:
                    return "GET_CACHE_RSP";
                default:
                    return "unknown type (" + type + ')';
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeObject(mbr);
            out.writeObject(sock_addr);
            out.writeObject(cachedAddrs);
            out.writeObject(mbrs);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            mbr=(Address)in.readObject();
            sock_addr=(IpAddress)in.readObject();
            cachedAddrs=(Map<Address,IpAddress>)in.readObject();
            mbrs=(Set<Address>)in.readObject();
        }

        public int size() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Util.size(mbr);
            retval+=Util.size(sock_addr);

            retval+=Global.INT_SIZE; // cachedAddrs size
            Address key;
            IpAddress val;
            if(cachedAddrs != null) {
                for(Map.Entry<Address,IpAddress> entry: cachedAddrs.entrySet()) {
                    if((key=entry.getKey()) != null)
                        retval+=Util.size(key);
                    retval+=Global.BYTE_SIZE; // presence for val
                    if((val=entry.getValue()) != null)
                        retval+=val.size();
                }
            }

            retval+=Global.INT_SIZE; // mbrs size
            if(mbrs != null) {
                for(Address m: mbrs) {
                    retval+=Util.size(m);
                }
            }

            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            int size;
            out.writeByte(type);
            Util.writeAddress(mbr, out);
            Util.writeStreamable(sock_addr, out);
            size=cachedAddrs != null? cachedAddrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Map.Entry<Address,IpAddress> entry: cachedAddrs.entrySet()) {
                    Address key=entry.getKey();
                    IpAddress val=entry.getValue();
                    Util.writeAddress(key, out);
                    Util.writeStreamable(val, out);
                }
            }
            size=mbrs != null? mbrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Address address: mbrs) {
                    Util.writeAddress(address, out);
                }
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            int size;
            type=in.readByte();
            mbr=Util.readAddress(in);
            sock_addr=(IpAddress)Util.readStreamable(IpAddress.class, in);
            size=in.readInt();
            if(size > 0) {
                if(cachedAddrs == null)
                    cachedAddrs=new HashMap<Address,IpAddress>(size);
                for(int i=0; i < size; i++) {
                    Address key=Util.readAddress(in);
                    IpAddress val=(IpAddress)Util.readStreamable(IpAddress.class, in);
                    cachedAddrs.put(key, val);
                }
            }
            size=in.readInt();
            if(size > 0) {
                if(mbrs == null)
                    mbrs=new HashSet<Address>();
                for(int i=0; i < size; i++) {
                    Address addr=Util.readAddress(in);
                    mbrs.add(addr);
                }
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
    private class ServerSocketHandler implements Runnable {
        Thread acceptor=null;
        /** List<ClientConnectionHandler> */
        final List<ClientConnectionHandler> clients=new LinkedList<ClientConnectionHandler>();


        String getName() {
            return acceptor != null? acceptor.getName() : null;
        }
 
        ServerSocketHandler() {
            start();
        }

        final void start() {
            if(acceptor == null) {
                acceptor=getProtocolStack().getThreadFactory().newThread(this, "FD_SOCK server socket acceptor");                
                acceptor.setDaemon(true);                  
                acceptor.start();
            }
        }


        final void stop(boolean graceful) {
            if(acceptor != null && acceptor.isAlive()) {
                try {
                    srv_sock.close(); // this will terminate thread, peer will receive SocketException (socket close)
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
                    if(log.isTraceEnabled()) // +++ remove
                        log.trace("waiting for client connections on " + srv_sock.getInetAddress() + ":" +
                                  srv_sock.getLocalPort());
                    client_sock=srv_sock.accept();
                    if(log.isTraceEnabled()) // +++ remove
                        log.trace("accepted connection from " + client_sock.getInetAddress() + ':' + client_sock.getPort());
                    ClientConnectionHandler client_conn_handler=new ClientConnectionHandler(client_sock, clients);
                    Thread t = getProtocolStack().getThreadFactory().newThread(client_conn_handler, "FD_SOCK client connection handler");
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
    private static class ClientConnectionHandler implements Runnable {
        Socket                              client_sock=null;
        InputStream                         in;
        final Object                        mutex=new Object();
        final List<ClientConnectionHandler> clients=new ArrayList<ClientConnectionHandler>();

        ClientConnectionHandler(Socket client_sock, List<ClientConnectionHandler> clients) {           
            this.client_sock=client_sock;
            this.clients.addAll(clients);
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

        private void closeClientSocket() {
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
                int b=0;
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
    private class BroadcastTask implements Runnable {
        final Set<Address> suspected_mbrs=new HashSet<Address>();
        Future             future;


        /** Adds a suspected member. Starts the task if not yet running */
        public void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            if(!members.contains(mbr)) return;
            synchronized(suspected_mbrs) {
                if(suspected_mbrs.add(mbr))
                    startTask();
            }
        }


        public void removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return;
            if(log.isDebugEnabled()) log.debug("member is " + suspected_mbr);
            synchronized(suspected_mbrs) {
                suspected_mbrs.remove(suspected_mbr);
                if(suspected_mbrs.isEmpty()) {
                    stopTask();
                }
            }
        }


        public void removeAll() {
            synchronized(suspected_mbrs) {
                suspected_mbrs.clear();
                stopTask();
            }
        }
        

        private void startTask() {
            if(future == null || future.isDone()) {
                try {
                    future=timer.scheduleWithFixedDelay(this, suspect_msg_interval, suspect_msg_interval, TimeUnit.MILLISECONDS);
                }
                catch(RejectedExecutionException e) {
                    if(log.isWarnEnabled())
                        log.warn("task " + this + " was rejected as timer thread pool is shutting down");
                }
            }
        }

        private void stopTask() {
            if(future != null) {
                future.cancel(false);
                future=null;
            }
        }


        /**
         * Removes all elements from suspected_mbrs that are <em>not</em> in the new membership
         */
        public void adjustSuspectedMembers(Vector<Address> new_mbrship) {
            if(new_mbrship == null || new_mbrship.isEmpty()) return;
            synchronized(suspected_mbrs) {
                boolean modified=suspected_mbrs.retainAll(new_mbrship);
                if(log.isTraceEnabled() && modified)
                    log.trace("adjusted suspected_mbrs: " + suspected_mbrs);
                if(suspected_mbrs.isEmpty())
                    stopTask();
            }
        }


        public void run() {
            Message suspect_msg;
            FdHeader hdr;

            if(log.isDebugEnabled())
                log.debug("broadcasting SUSPECT message (suspected_mbrs=" + suspected_mbrs + ") to group");

            synchronized(suspected_mbrs) {
                if(suspected_mbrs.isEmpty()) {
                    stopTask();
                    if(log.isDebugEnabled()) log.debug("task done (no suspected members)");
                    return;
                }

                hdr=new FdHeader(FdHeader.SUSPECT);
                hdr.mbrs=new HashSet<Address>(suspected_mbrs);
            }
            suspect_msg=new Message();       // mcast SUSPECT to all members
            suspect_msg.setFlag(Message.OOB);
            suspect_msg.putHeader(name, hdr);
            down_prot.down(new Event(Event.MSG, suspect_msg));
            if(log.isDebugEnabled()) log.debug("task done");
        }
    }


}
