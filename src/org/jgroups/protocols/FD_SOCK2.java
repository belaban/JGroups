package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.blocks.cs.ConnectionListener;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * Failure detection protocol based on TCP connections, successor to {@link FD_SOCK}.
 * @author Bela Ban April 27 2021
 */
@MBean(description="Failure detection protocol based on sockets connecting members")
public class FD_SOCK2 extends Protocol implements Receiver, ConnectionListener, ProcessingQueue.Handler<FD_SOCK2.Request> {

    @LocalAddress
    @Property(description="The NIC on which the ServerSocket should listen on. " +
      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress                    bind_addr;

    @Property(description="Offset from the transport's bind port")
    protected int                            offset=100;

    @Property(description="Number of ports to probe for finding a free port")
    protected int                            port_range=3;

    @Property(description="Start port for client socket. Default value of 0 picks a random port")
    protected int                            client_bind_port;

    @Property(description="Use \"external_addr\" if you have hosts on different networks behind firewalls. On each " +
      "firewall, set up a port forwarding rule to the local IP (e.g. 192.168.1.100) of the host, then on each host, " +
      "set the \"external_addr\" TCP transport attribute to the external (public IP) address of the firewall",
              systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress                    external_addr;

    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
              systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int                            external_port;

    @Property(description="Interval for broadcasting suspect messages",type=AttributeType.TIME)
    protected long                           suspect_msg_interval=5000;

    @Property(description="Max time (ms) to wait for a connect attempt",type=AttributeType.TIME)
    protected int                            connect_timeout=1000;

    @Property(description="The lowest port the FD_SOCK server can listen on. Needed when wrapping around, looking " +
      "for ports. See https://issues.redhat.com/browse/JGRP-2560 for details")
    protected int                            min_port=1024;

    @Property(description="The highest port the FD_SOCK server can listen on. Needed when wrapping around, looking " +
      "for ports. See https://issues.redhat.com/browse/JGRP-2560 for details.")
    protected int                            max_port=0xFFFF+1; // 65536

    @ManagedAttribute(description="Number of suspect events emitted")
    protected int                            num_suspect_events;

    @ManagedAttribute(description="True when this member is leaving the cluster, set to false when joining")
    protected volatile boolean               shutting_down;

    @ManagedAttribute(description="List of pingable members of a cluster")
    protected final Membership               pingable_mbrs=new Membership();

    @ManagedAttribute(description="List of the current cluster members")
    protected final Membership               members=new Membership();

    @ManagedAttribute(description="List of currently suspected members")
    protected final Membership               suspected_mbrs=new Membership();

    @ManagedAttribute(description="The cluster we've joined. Set on joining a cluster, null when unconnected")
    protected String                         cluster;

    protected NioServer                      srv;
    protected final PingDest                 ping_dest=new PingDest(); // address of the member we monitor
    protected TimeScheduler                  timer;
    protected final BroadcastTask            bcast_task=new BroadcastTask(); // to resend SUSPECT message (until view change)
    protected final ProcessingQueue<Request> req_handler=new ProcessingQueue<Request>().setHandler(this);
    protected final BoundedList<String>      suspect_history=new BoundedList<>(20);



    public FD_SOCK2() {
    }


    @ManagedAttribute(description="The number of currently suspected members")
    public int         getNumSuspectedMembers()          {return suspected_mbrs.size();}

    @ManagedAttribute(description="Ping destination")
    public String      getPingDest()                     {return String.format("%s", ping_dest);}

    @ManagedAttribute(description="The client state (CONNECTED / DISCONNECTED)")
    public String      getClientState()                  {return ping_dest.clientState().toString();}

    public InetAddress getBindAddress()                  {return bind_addr;}
    public FD_SOCK2    setBindAddress(InetAddress b)     {this.bind_addr=b; return this;}

    public InetAddress getExternalAddress()              {return external_addr;}
    public FD_SOCK2    setExternalAddress(InetAddress e) {this.external_addr=e; return this;}

    public int         getExternalPort()                 {return external_port;}
    public FD_SOCK2    setExternalPort(int e)            {this.external_port=e; return this;}

    public long        getSuspectMsgInterval()           {return suspect_msg_interval;}
    public FD_SOCK2    setSuspectMsgInterval(long s)     {this.suspect_msg_interval=s; return this;}

    public int         getClientBindPort()               {return client_bind_port;}
    public FD_SOCK2    setClientBindPort(int c)          {this.client_bind_port=c; return this;}

    public int         getPortRange()                    {return port_range;}
    public FD_SOCK2    setPortRange(int p)               {this.port_range=p; return this;}

    @ManagedAttribute(description="Actual port the server is listening on")
    public int getActualBindPort() {
        Address addr=srv != null? srv.localAddress() : null;
        return addr != null? ((IpAddress)addr).getPort() : 0;
    }

    @ManagedOperation(description="Print suspect history")
    public String printSuspectHistory() {return String.join("\n", suspect_history);}

    @ManagedOperation(description="Prints the connections to other FD_SOCK2 instances")
    public String printConnections() {return srv.printConnections();}


    public void start() throws Exception {
        super.start();
        TP transport=getTransport();
        timer=transport.getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        PhysicalAddress addr=transport.getPhysicalAddress();
        int actual_port=((IpAddress)addr).getPort();
        int[] bind_ports=computeBindPorts(actual_port);
        srv=createServer(bind_ports);
        srv.receiver(this).clientBindPort(client_bind_port).usePeerConnections(true).addConnectionListener(this);
        srv.start();
        log.info("server listening on %s", bind_addr != null? srv.getChannel().getLocalAddress() : "*." + getActualBindPort());
    }

    public void stop() {
        Util.close(srv); // calls stop()
        pingable_mbrs.clear();
        suspected_mbrs.clear();
        bcast_task.clear();
        ping_dest.reset();
    }


    public void resetStats() {
        super.resetStats();
        num_suspect_events=0;
        suspect_history.clear();
    }




    public Object up(Message msg) {
        FdHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);  // message did not originate from FD_SOCK2 layer, just pass up
        return handle(hdr, msg.getSrc());
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            FdHeader hdr=msg.getHeader(id);
            if(hdr != null) {
                it.remove();
                handle(hdr, msg.getSrc());
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
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
                shutting_down=false;
                cluster=evt.getArg();
                break;

            case Event.DISCONNECT:
                shutting_down=true;
                cluster=null;
                break;

            case Event.VIEW_CHANGE:
                Object ret=down_prot.down(evt);
                handleView(evt.arg());
                return ret;
        }
        return down_prot.down(evt);
    }

    public void receive(Address sender, byte[] buf, int offset, int length) {
        try {
            receive(sender, new ByteArrayDataInputStream(buf, offset, length));
        }
        catch(Exception e) {
            log.error("failed handling message received from " + sender, e);
        }
    }

    public void receive(Address sender, DataInput in) throws Exception {
        Message msg=new EmptyMessage();
        msg.readFrom(in);
        FdHeader hdr=msg.getHeader(id);
        if(hdr == null)
            throw new IllegalStateException(String.format("message from %s does not have a header (id=%d)", sender, id));
        switch(hdr.type) {
            case FdHeader.SUSPECT: case FdHeader.UNSUSPECT: // I should not receive these!
                break;

            case FdHeader.CONNECT:
                log.trace("%s: CONNECT <-- %s", local_addr, msg.src());
                FdHeader h=new FdHeader(FdHeader.CONNECT_RSP).cluster(cluster).serverAddress(local_addr);
                Message rsp=new EmptyMessage().setSrc(local_addr).putHeader(id, h);
                ByteArray buf=messageToBuffer(rsp);
                log.trace("%s: CONNECT-RSP[cluster=%s, srv=%s] --> %s", local_addr, cluster, local_addr, msg.src());
                srv.send(sender, buf.getArray(), buf.getOffset(), buf.getLength());
                break;

            case FdHeader.CONNECT_RSP:
                log.trace("%s: CONNECT-RSP <-- %s [cluster=%s, srv=%s]",
                          local_addr, msg.src(), hdr.cluster, hdr.srv);
                if(Objects.equals(this.cluster, hdr.cluster) && Objects.equals(hdr.srv, ping_dest.dest())) {
                    ping_dest.clientState(State.CONNECTED).setConnectResult(true);
                }
                else {
                    log.trace("%s: addresses don't match: my ping-dest=%s, server's address=%s",
                             local_addr, ping_dest.dest(), hdr.srv);
                    ping_dest.setConnectResult(false);
                }
                break;

            default:
                throw new IllegalStateException(String.format("type %d not known", hdr.type));
        }
    }

    public void connectionEstablished(Connection conn) {
        log.trace("%s: created connection to %s", local_addr, conn.peerAddress());
    }



    public void connectionClosed(Connection conn) {
        Address peer=conn != null? conn.peerAddress() : null;
        if(peer != null && Objects.equals(peer, ping_dest.destPhysical()) && !shutting_down) {
            Address dest=ping_dest.dest();
            if(dest != null) {
                log.debug("%s: connection to %s closed", local_addr, dest);
                pingable_mbrs.remove(dest);
                req_handler.add(new Request(Request.Type.ConnectToNextPingDest, dest));
            }
        }
    }

    protected Object handle(FdHeader hdr, Address sender) {
        switch(hdr.type) {
            case FdHeader.SUSPECT:
                if(hdr.mbrs != null) {
                    log.trace("%s: received SUSPECT message from %s: suspects=%s", local_addr, sender, hdr.mbrs);
                    suspect(hdr.mbrs);
                }
                break;

            case FdHeader.UNSUSPECT:
                if(hdr.mbrs != null) {
                    log.trace("%s: received UNSUSPECT message from %s: mbrs=%s", local_addr, sender, hdr.mbrs);
                    hdr.mbrs.forEach(this::unsuspect);
                    req_handler.add(new Request(Request.Type.ConnectToNextPingDest, null));
                }
                break;
        }
        return null;
    }

    protected NioServer createServer(int[] bind_ports) {
        DefaultSocketFactory socket_factory=new DefaultSocketFactory();
        DefaultThreadFactory thread_factory=new DefaultThreadFactory("nio", false);
        thread_factory.useFibers(getTransport().useVirtualThreads());
        for(int bind_port: bind_ports) {
            try {
                return new NioServer(thread_factory, socket_factory, bind_addr, bind_port, bind_port,
                                     external_addr, external_port, 0);
            }
            catch(Exception ignored) {
            }
        }
        throw new IllegalStateException(String.format("%s: failed to find an available port in ports %s",
                                                      local_addr, Arrays.toString(bind_ports)));
    }

    protected void closeConnectionToPingDest() {
        if(!ping_dest.connected())
            return;
        try {
            ping_dest.clientState(State.DISCONNECTED);
            if(srv.closeConnection(ping_dest.destPhysical(), false) == false)
                return;
            log.debug("%s: connection to %s closed", local_addr, ping_dest);
        }
        finally {
            ping_dest.reset();
        }
    }

    public void handle(Request req) throws Exception {
        switch(req.type) {
            case ConnectToNextPingDest:
                connectToNextPingDest(req.suspect);
                break;
            case CloseConnectionToPingDest:
                closeConnectionToPingDest();
                break;
        }
    }

    protected void handleView(View v) {
        final List<Address> new_mbrs=v.getMembers();
        members.set(new_mbrs);
        suspected_mbrs.retainAll(new_mbrs);
        bcast_task.adjustSuspects(new_mbrs);
        pingable_mbrs.set(new_mbrs);
        if(v.size() < 2)
            req_handler.add(new Request(Request.Type.CloseConnectionToPingDest, null));
        else
            req_handler.add(new Request(Request.Type.ConnectToNextPingDest, null));
    }


    protected void connectToNextPingDest(Address already_suspect) {
        List<Address> suspected_members=new ArrayList<>();
        if(already_suspect != null)
            suspected_members.add(already_suspect);
        while(!pingable_mbrs.isEmpty()) { // connectTo() removes unreachable members from pingable_mbrs
            Address new_ping_dest=pingable_mbrs.getNext(local_addr); // get the neighbor to my right
            boolean hasNewPingDest=ping_dest.destChanged(new_ping_dest);
            if(hasNewPingDest) {
                if(connectTo(new_ping_dest, this.members)) // also sets ping_dest.dest if successful
                    break;
                else {
                    if(!ping_dest.connected()) {
                        pingable_mbrs.remove(new_ping_dest);
                        suspected_members.add(new_ping_dest);
                    }
                }
            }
            else
                break;
        }
        if(!suspected_members.isEmpty())
            broadcastSuspectMessage(suspected_members);
    }

    protected boolean connectTo(Address new_ping_dest, Membership mbrs) {
        Address old_dest=ping_dest.dest();
        IpAddress old_dest_physical=ping_dest.destPhysical();
        List<IpAddress> dests=getPhysicalAddresses(new_ping_dest);
        ping_dest.reset().dest(new_ping_dest);
        log.debug("%s: trying to connect to %s", local_addr, new_ping_dest);
        long start=System.currentTimeMillis();
        for(IpAddress d: dests) {
            if(connectTo(d, new_ping_dest)) {
                long time=System.currentTimeMillis() - start;
                ping_dest.dest(new_ping_dest).destPhysical(d);
                log.debug("%s: connected successfully to %s (%s) in %d ms", local_addr, ping_dest.dest(), d, time);

                // Close the connection to the previous ping_dest if it was *not* our neighbor to the left:
                // 1. {A,B}: A - B, B - A // '-' denotes a bidirectional connection
                // 2. {A,B,C}: A - B, B - C, C - A
                // 3. {A,B,C,D}: A - B, B - C, C - D, D - A: C can now close its connection to A
                Address left_mbr=mbrs.getPrevious(local_addr);
                if(old_dest != null && !Objects.equals(old_dest, left_mbr)) {
                    srv.closeConnection(old_dest_physical, false); // close connection to previous ping_dest
                    log.trace("%s: closed connection to previous ping-dest %s (%s)",
                              local_addr, old_dest, old_dest_physical);
                }
                return true;
            }
        }
        return false;
    }

    protected boolean connectTo(IpAddress dest, Address logical_addr) {
        Message msg=new EmptyMessage().setSrc(local_addr).putHeader(id, new FdHeader(FdHeader.CONNECT).serverAddress(logical_addr));
        try {
            ByteArray buf=messageToBuffer(msg);
            log.trace("%s: CONNECT --> %s (%s)", local_addr, logical_addr, dest);
            ping_dest.resetConnectResult();
            boolean existing_connection=srv.hasConnection(dest);
            srv.send(dest, buf.getArray(), buf.getOffset(), buf.getLength());
            ping_dest.waitForConnect(connect_timeout); // returns on CONNECT-RSP, connection exception or timeout
            if(ping_dest.connected())
                return true;
            if(!existing_connection)  // close a new connection
                srv.closeConnection(dest);
            return false;
        }
        catch(Exception ex) {
            log.trace("%s: failed connecting to %s: %s", local_addr, dest, ex.getMessage());
            return false;
        }
    }

    /** Returns the physical addresses for a in range [a+offset..a+offset+port_range */
    protected List<IpAddress> getPhysicalAddresses(Address a) {
        IpAddress pa=(IpAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, a));
        InetAddress addr=pa.getIpAddress();
        int actual_port=pa.getPort();
        int[] bind_ports=computeBindPorts(actual_port);
        return IntStream.of(bind_ports).boxed().map(p -> new IpAddress(addr, p)).collect(Collectors.toList());
    }

    public static ByteArray messageToBuffer(Message msg) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(msg.size());
        msg.writeTo(out);
        return out.getBuffer();
    }

    protected int[] computeBindPorts(int actual_port) {
        int[] bind_ports=new int[port_range];
        for(int i=0; i < port_range; i++) {
            int port=(actual_port+offset+i) % max_port;
            if(port < min_port)
                port=port+min_port;
            bind_ports[i]=port;
        }
        return bind_ports;
    }


    protected void suspect(Collection<Address> suspects) {
        if(suspects == null)
            return;

        suspects.remove(local_addr);
        suspects.forEach(suspect -> suspect_history.add(String.format("%s: %s", new Date(), suspect)));
        suspected_mbrs.add(suspects);
        Collection<Address> suspects_copy=suspected_mbrs.getMembers(); // returns a copy
        if(suspects_copy.isEmpty())
            return;

        // Check if we're coord, then send up the stack, make a copy (https://issues.redhat.com/browse/JGRP-2552)
        Membership eligible_mbrs=this.members.copy().remove(suspected_mbrs.getMembers());
        if(eligible_mbrs.isCoord(local_addr)) {
            log.debug("%s: suspecting %s", local_addr, suspects_copy);
            up_prot.up(new Event(Event.SUSPECT, suspects_copy));
            down_prot.down(new Event(Event.SUSPECT, suspects_copy));
        }
    }

    protected void unsuspect(Address mbr) {
        if(mbr == null)
            return;
        suspected_mbrs.remove(mbr);
        bcast_task.removeSuspect(mbr);
        pingable_mbrs.add(mbr);
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
    protected void broadcastSuspectMessage(List<Address> suspected_members) {
        if(suspected_members == null || suspected_members.isEmpty())
            return;

        log.debug("%s: broadcasting suspect(%s)", local_addr, suspected_members);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        FdHeader hdr=new FdHeader(FdHeader.SUSPECT).mbrs(suspected_members);
        Message suspect_msg=new EmptyMessage().putHeader(this.id, hdr);
        down_prot.down(suspect_msg);

        // 2. Add to broadcast task and start if not running. The task ends when suspected mbrs are removed from mbrship
        bcast_task.addSuspects(suspected_members);
        if(stats)
            num_suspect_events++;
    }


    protected void broadcastUnuspectMessage(Address mbr) {
        if(mbr == null) return;

        log.debug("%s: broadcasting unsuspect(%s)", local_addr, mbr);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        FdHeader hdr=new FdHeader(FdHeader.UNSUSPECT).mbrs(Collections.singleton(mbr));
        Message suspect_msg=new EmptyMessage().putHeader(this.id, hdr);
        down_prot.down(suspect_msg);
    }



    protected enum State {DISCONNECTED, CONNECTED}

    protected static class PingDest {
        protected Address                dest;
        protected IpAddress              dest_physical;
        protected State                  client_state=State.DISCONNECTED;
        protected final Promise<Boolean> connect_promise=new Promise<>();

        protected Address   dest()                      {return dest;}
        protected PingDest  dest(Address d)             {dest=d; return this;}
        protected IpAddress destPhysical()              {return dest_physical;}
        protected PingDest  destPhysical(IpAddress d)   {dest_physical=d; return this;}
        protected State     clientState()               {return client_state;}
        protected PingDest  clientState(State s)        {client_state=s; return this;}
        protected boolean   connected()                 {return client_state == State.CONNECTED;}
        protected boolean   destChanged(Address a)      {return a != null && !Objects.equals(a, dest);}
        protected boolean   waitForConnect(long time)   {return connect_promise.getResult(time);}
        protected PingDest  setConnectResult(boolean b) {connect_promise.setResult(b); return this;}
        protected PingDest  resetConnectResult()        {connect_promise.reset(true); return this;}

        protected PingDest  reset() {
            dest=dest_physical=null; client_state=State.DISCONNECTED;
            connect_promise.reset(true);
            return this;
        }

        public String toString() {
            return String.format("%s [%s %s]", dest, dest_physical, client_state);
        }
    }


    public static class FdHeader extends Header {
        public static final byte SUSPECT        = 1; // mbrs
        public static final byte UNSUSPECT      = 2; // mbrs
        public static final byte CONNECT        = 3;
        public static final byte CONNECT_RSP    = 4; // cluster, local addr of server

        protected byte                  type;
        protected Collection<Address>   mbrs;
        protected Address               srv;      // address of the server, set on CONNECT_RSP
        protected String                cluster;  // cluster of the server, set on CONNECT_RSP


        public FdHeader() {
        }

        public FdHeader(byte type)                                    {this.type=type;}
        public short                      getMagicId()                {return 93;}
        public Supplier<? extends Header> create()                    {return FdHeader::new;}
        public FdHeader                   mbrs(Collection<Address> m) {this.mbrs=m; return this;}
        public FdHeader                   serverAddress(Address a)    {srv=a; return this;}
        public FdHeader                   cluster(String name)        {cluster=name; return this;}



        @Override
        public int serializedSize() {
            int retval=Global.BYTE_SIZE + Global.INT_SIZE; // type + mbrs size
            if(mbrs != null)
                for(Address m: mbrs)
                    retval+=Util.size(m);
            retval+=Util.size(cluster) + Util.size(srv);
            return retval;

        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            int size=mbrs != null? mbrs.size() : 0;
            out.writeInt(size);
            if(size > 0)
                for(Address address: mbrs)
                    Util.writeAddress(address, out);
            Bits.writeString(cluster, out);
            Util.writeAddress(srv, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            int size=in.readInt();
            if(size > 0) {
                mbrs=new HashSet<>();
                for(int i=0; i < size; i++)
                    mbrs.add(Util.readAddress(in));
            }
            cluster=Bits.readString(in);
            srv=Util.readAddress(in);
        }

        public String toString() {
            return String.format("%s%s%s%s", type2String(type), mbrs != null? ", mbrs=" + mbrs : "",
                                 srv != null? ", srv=" + srv : "", cluster != null? ", cluster=" + cluster : "");
        }

        protected static String type2String(byte type) {
            switch(type) {
                case SUSPECT:     return "SUSPECT";
                case UNSUSPECT:   return "UNSUSPECT";
                case CONNECT:     return "CONNECT";
                case CONNECT_RSP: return "CONNECT_RSP";
                default:          return "unknown type (" + type + ')';
            }
        }
    }



    protected static class Request {
        enum Type {ConnectToNextPingDest, CloseConnectionToPingDest};
        protected final Type    type;
        protected final Address suspect;

        public Request(Type type, Address suspect) {
            this.type=type;
            this.suspect=suspect;
        }

        public String toString() {
            return String.format("%s (suspect=%s)", type, suspect);
        }
    }


    /**
     * Task which periodically broadcasts a list of suspected members. The goal is not to lose a SUSPECT message: since
     * these are broadcast unreliably, they might get dropped. The BroadcastTask makes sure they are retransmitted until
     * a view has been received which doesn't contain the suspected members any longer. Then the task terminates.
     */
    protected class BroadcastTask implements Runnable {
        protected final Set<Address> suspects=new HashSet<>();
        protected Future<?>          future;


        protected synchronized void addSuspects(List<Address> mbrs) {
            if(mbrs == null || mbrs.isEmpty()) return;
            List<Address> tmp=new ArrayList<>(mbrs);
            tmp.retainAll(members.getMembers()); // removes non-members from mbrs; copy is required or else CCME
            if(suspects.addAll(tmp))
                startTask();
        }

        protected synchronized void removeSuspect(Address suspect) {
            if(suspect == null) return;
            if(suspects.remove(suspect) && suspects.isEmpty())
                stopTask();
        }

        /** Removes all elements from suspects that are <em>not</em> in the new membership */
        protected synchronized void adjustSuspects(List<Address> mbrs) {
            if(mbrs == null || mbrs.isEmpty()) return;
            if(suspects.retainAll(mbrs) && suspects.isEmpty())
                stopTask();
        }

        protected synchronized void clear() {
            suspects.clear();
            stopTask();
        }

        @GuardedBy("this")
        protected void startTask() {
            if(future == null || future.isDone()) {
                future=timer.scheduleWithFixedDelay(this, suspect_msg_interval, suspect_msg_interval, MILLISECONDS,
                                                    getTransport() instanceof TCP);
            }
        }

        @GuardedBy("this")
        protected void stopTask() {
            if(future != null) {
                future.cancel(false);
                future=null;
            }
        }

        public void run() {
            log.trace("%s: broadcasting SUSPECT message (suspected_mbrs=%s)", local_addr, suspects);
            FdHeader hdr;
            synchronized(this) {
                if(suspects.isEmpty()) {
                    stopTask();
                    return;
                }
                hdr=new FdHeader(FdHeader.SUSPECT).mbrs(new HashSet<>(suspects));
            }
            // mcast SUSPECT to all members
            Message suspect_msg=new EmptyMessage().putHeader(id, hdr);
            down_prot.down(suspect_msg);
        }

        public String toString() {
            return FD_SOCK2.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }


}
