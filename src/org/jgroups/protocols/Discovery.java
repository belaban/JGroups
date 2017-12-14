package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * The Discovery protocol retrieves the initial membership (used by GMS and MERGE3) by sending discovery requests.
 * We do this in subclasses of Discovery, e.g. by mcasting a discovery request ({@link PING}) or, if gossiping is enabled,
 * by contacting the GossipRouter ({@link TCPGOSSIP}).<p/>
 * The responses should allow us to determine the coordinator which we have to contact, e.g. in case we want to join
 * the group, or to see if we have diverging views in case of MERGE2.<p/>
 * When we are a server (after having received the BECOME_SERVER event), we'll respond to discovery requests with
 * a discovery response.
 *
 * @author Bela Ban
 */
@MBean
public abstract class Discovery extends Protocol {


    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Return from the discovery phase as soon as we have 1 coordinator response")
    protected boolean                    break_on_coord_rsp=true;

    @Property(description="Whether or not to return the entire logical-physical address cache mappings on a " +
      "discovery request, or not.")
    protected boolean                    return_entire_cache;

    @Property(description="If greater than 0, we'll wait a random number of milliseconds in range [0..stagger_timeout] " +
      "before sending a discovery response. This prevents traffic spikes in large clusters when everyone sends their " +
      "discovery response at the same time")
    protected long                       stagger_timeout;


    @Property(description="If a persistent disk cache (PDC) is present, combine the discovery results with the " +
      "contents of the disk cache before returning the results")
    protected boolean                    use_disk_cache;

    @Property(description="Max size of the member list shipped with a discovery request. If we have more, the " +
      "mbrs field in the discovery request header is nulled and members return the entire membership, " +
      "not individual members")
    protected int                        max_members_in_discovery_request=500;

    @Property(description="Expiry time of discovery responses in ms")
    protected long                       discovery_rsp_expiry_time=60000;

    @Property(description="If true then the discovery is done on a separate timer thread. Should be set to true when " +
      "discovery is blocking and/or takes more than a few milliseconds")
    protected boolean                    async_discovery;

    @Property(description="If enabled, use a separate thread for every discovery request. Can be used with or without " +
      "async_discovery")
    protected boolean                    async_discovery_use_separate_thread_per_request;

    @Property(description="When a new node joins, and we have a static discovery protocol (TCPPING), then send the " +
      "contents of the discovery cache to new and existing members if true (and we're the coord). Addresses JGRP-1903")
    protected boolean                    send_cache_on_join=true;

    @Property(description="The max rank of this member to respond to discovery requests, e.g. if " +
      "max_rank_to_reply=2 in {A,B,C,D,E}, only A (rank 1) and B (rank 2) will reply. A value <= 0 means " +
      "everybody will reply. This attribute is ignored if TP.use_ip_addrs is false.")
    protected int                        max_rank_to_reply;

    /* ---------------------------------------------   JMX      ------------------------------------------------------ */

    @ManagedAttribute(description="Total number of discovery requests sent ")
    protected int                        num_discovery_requests;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected volatile boolean           is_server;
    protected volatile boolean           is_leaving;
    protected TimeScheduler              timer;
    protected volatile View              view;
    @ManagedAttribute(description="Whether this member is the current coordinator")
    protected volatile boolean           is_coord;
    protected volatile Address           local_addr;
    protected volatile Address           current_coord;
    protected String                     cluster_name;
    protected final Map<Long,Responses>  ping_responses=new HashMap<>();
    @ManagedAttribute(description="Whether the transport supports multicasting")
    protected boolean                    transport_supports_multicasting=true;
    protected boolean                    use_ip_addrs; // caches TP.use_ip_addrs
    @ManagedAttribute(description="True if sending a message can block at the transport level")
    protected boolean                    sends_can_block=true;
    protected static final byte[]        WHITESPACE=" \t".getBytes();



    public void init() throws Exception {
        TP tp=getTransport();
        timer=tp.getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved from protocol stack");
        if(stagger_timeout < 0)
            throw new IllegalArgumentException("stagger_timeout cannot be negative");
        transport_supports_multicasting=tp.supportsMulticasting();
        sends_can_block=getTransport() instanceof TCP; // UDP and TCP_NIO2 won't block
        use_ip_addrs=tp.getUseIpAddresses();
    }

    public abstract boolean isDynamic();


    public void handleDisconnect() {
    }

    public void handleConnect() {
    }

    public void discoveryRequestReceived(Address sender, String logical_name, PhysicalAddress physical_addr) {

    }

    public int       getNumberOfDiscoveryRequestsSent() {return num_discovery_requests;}
    public boolean   breakOnCoordResponse()             {return break_on_coord_rsp;}
    public Discovery breakOnCoordResponse(boolean flag) {break_on_coord_rsp=flag; return this;}
    public boolean   returnEntireCache()                {return return_entire_cache;}
    public Discovery returnEntireCache(boolean flag)    {return_entire_cache=flag; return this;}
    public long      staggerTimeout()                   {return stagger_timeout;}
    public Discovery staggerTimeout(long timeout)       {stagger_timeout=timeout; return this;}
    public boolean   useDiskCache()                     {return use_disk_cache;}
    public Discovery useDiskCache(boolean flag)         {use_disk_cache=flag; return this;}
    public Discovery discoveryRspExpiryTime(long t)     {this.discovery_rsp_expiry_time=t; return this;}



    @ManagedAttribute
    public String getView() {return view != null? view.getViewId().toString() : "null";}

    public ViewId getViewId() {
        return view != null? view.getViewId() : null;
    }

    @ManagedAttribute(description="The address of the current coordinator")
    public String getCurrentCoord() {return current_coord != null? current_coord.toString() : "n/a";}

    protected boolean isMergeRunning() {
        Object retval=up_prot.up(new Event(Event.IS_MERGE_IN_PROGRESS));
        return retval instanceof Boolean && (Boolean)retval;
    }

    @ManagedOperation(description="Sends information about my cache to everyone but myself")
    public void sendCacheInformation() {
        List<Address> current_members=new ArrayList<>(view.getMembers());
        disseminateDiscoveryInformation(current_members, null, current_members);
    }

    public List<Integer> providedUpServices() {
        return Arrays.asList(Event.FIND_INITIAL_MBRS, Event.GET_PHYSICAL_ADDRESS, Event.FIND_MBRS);
    }

    public void resetStats() {
        super.resetStats();
        num_discovery_requests=0;
    }

    public void start() throws Exception {
        super.start();
    }

    public void stop() {
        is_server=false;
    }

    public void addResponse(Responses rsp) {
        synchronized(ping_responses) {
            ping_responses.put(System.nanoTime(), rsp);
        }
    }

    /**
     * Fetches information (e.g. physical address, logical name) for the given member addresses. Needs to add responses
     * to the {@link org.jgroups.util.Responses} object. If {@link #async_discovery} is true, this method will be called
     * in a separate thread, otherwise the caller's thread will be used.
     * @param members A list of logical addresses (typically {@link org.jgroups.util.UUID}s). If null, then information
     *                for all members is fetched
     * @param initial_discovery Set to true if this is for the initial membership discovery. Some protocols (e.g.
     *                          file based ones) may return only the information for the coordinator(s).
     * @param responses The list to which responses should be added
     */
    protected abstract void findMembers(List<Address> members, boolean initial_discovery, Responses responses);

    public Responses findMembers(final List<Address> members, final boolean initial_discovery, boolean async) {
        num_discovery_requests++;
        int num_expected=members != null? members.size() : 0;
        int capacity=members != null? members.size() : 16;
        final Responses rsps=new Responses(num_expected, initial_discovery && break_on_coord_rsp, capacity);
        synchronized(ping_responses) {
            ping_responses.put(System.nanoTime(), rsps);
        }
        if(async || async_discovery)
            timer.execute(() -> findMembers(members, initial_discovery, rsps));
        else
            findMembers(members, initial_discovery, rsps);
        weedOutCompletedDiscoveryResponses();
        return rsps;
    }


    @ManagedOperation(description="Runs the discovery protocol to find initial members")
    public String findInitialMembersAsString() {
        Responses rsps=findMembers(null, false, false);
        if(!rsps.isDone())
            rsps.waitFor(300);
        if(rsps.isEmpty()) return "<empty>";
        StringBuilder sb=new StringBuilder();
        for(PingData rsp: rsps)
            sb.append(rsp).append("\n");
        return sb.toString();
    }


    @ManagedOperation(description="Reads logical-physical address mappings and logical name mappings from a " +
      "file (or URL) and adds them to the local caches")
    public void addToCache(String filename) throws Exception {
        InputStream in=ConfiguratorFactory.getConfigStream(filename);
        List<PingData> list=read(in);
        if(list != null)
            for(PingData data: list)
                addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
    }

    @ManagedOperation(description="Reads data from local caches and dumps them to a file")
    public void dumpCache(String output_filename) throws Exception {
        Map<Address,PhysicalAddress> cache_contents=
          (Map<Address,PhysicalAddress>)down_prot.down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS, false));

        List<PingData> list=new ArrayList<>(cache_contents.size());
        for(Map.Entry<Address,PhysicalAddress> entry: cache_contents.entrySet()) {
            Address         addr=entry.getKey();
            PhysicalAddress phys_addr=entry.getValue();
            PingData data=new PingData(addr, true, NameCache.get(addr), phys_addr).coord(addr.equals(local_addr));
            list.add(data);
        }
        OutputStream out=new FileOutputStream(output_filename);
        write(list, out);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.FIND_MBRS:
                return findMembers(evt.getArg(), false, true); // this is done asynchronously
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        PingHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);

        if(is_leaving)
            return null; // prevents merging back a leaving member (https://issues.jboss.org/browse/JGRP-1336)

        PingData data=readPingData(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
        Address logical_addr=data != null? data.getAddress() : msg.src();

        switch(hdr.type) {

            case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
                if(cluster_name == null || hdr.cluster_name == null) {
                    log.warn("cluster_name (%s) or cluster_name of header (%s) is null; passing up discovery " +
                               "request from %s, but this should not be the case", cluster_name, hdr.cluster_name, msg.src());
                }
                else {
                    if(!cluster_name.equals(hdr.cluster_name)) {
                        log.warn("%s: discarding discovery request for cluster '%s' from %s; " +
                                   "our cluster name is '%s'. Please separate your clusters properly",
                                 logical_addr, hdr.cluster_name, msg.src(), cluster_name);
                        return null;
                    }
                }

                // add physical address and logical name of the discovery sender (if available) to the cache
                if(data != null) { // null if use_ip_addrs is true
                    addDiscoveryResponseToCaches(logical_addr, data.getLogicalName(), data.getPhysicalAddr());
                    discoveryRequestReceived(msg.getSrc(), data.getLogicalName(), data.getPhysicalAddr());
                    addResponse(data, false);
                }

                if(return_entire_cache) {
                    Map<Address,PhysicalAddress> cache=(Map<Address,PhysicalAddress>)down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS));
                    if(cache != null) {
                        for(Map.Entry<Address,PhysicalAddress> entry: cache.entrySet()) {
                            Address addr=entry.getKey();
                            // JGRP-1492: only return our own address, and addresses in view.
                            if(addr.equals(local_addr) || (view != null && view.containsMember(addr))) {
                                PhysicalAddress physical_addr=entry.getValue();
                                sendDiscoveryResponse(addr, physical_addr, NameCache.get(addr), msg.getSrc(), isCoord(addr));
                            }
                        }
                    }
                    return null;
                }

                // Only send a response if hdr.mbrs is not empty and contains myself. Otherwise always send my info
                Collection<? extends Address> mbrs=data != null? data.mbrs() : null;
                boolean drop_because_of_rank=use_ip_addrs && max_rank_to_reply > 0 && hdr.initialDiscovery() && Util.getRank(view, local_addr) > max_rank_to_reply;
                if(drop_because_of_rank || (mbrs != null && !mbrs.contains(local_addr)))
                    return null;
                PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
                sendDiscoveryResponse(local_addr, physical_addr, NameCache.get(local_addr), msg.getSrc(), is_coord);
                return null;

            case PingHeader.GET_MBRS_RSP:
                // add physical address (if available) to transport's cache
                if(data != null) {
                    log.trace("%s: received GET_MBRS_RSP from %s: %s", local_addr, msg.src(), data);
                    handleDiscoveryResponse(data, msg.src());
                }
                return null;

            default:
                log.warn("got PING header with unknown type %d", hdr.type);
                return null;
        }
    }


    protected void handleDiscoveryResponse(PingData data, Address sender) {
        // add physical address (if available) to transport's cache
        Address logical_addr=data.getAddress() != null? data.getAddress() : sender;
        addDiscoveryResponseToCaches(logical_addr, data.getLogicalName(), data.getPhysicalAddr());
        boolean overwrite=Objects.equals(logical_addr, sender);
        addResponse(data, overwrite);
    }


    @SuppressWarnings("unchecked")
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.FIND_INITIAL_MBRS:      // sent by GMS layer
                return findMembers(null, true, false); // triggered by JOIN process (ClientGmsImpl)

            case Event.FIND_MBRS:
                return findMembers(evt.getArg(), false, false); // triggered by MERGE3

            case Event.VIEW_CHANGE:
                View old_view=view;
                view=evt.getArg();
                current_coord=view.getCoord();
                is_coord=current_coord != null && local_addr != null && current_coord.equals(local_addr);
                Object retval=down_prot.down(evt);
                if(send_cache_on_join && !isDynamic() && is_coord) {
                    List<Address> curr_mbrs, left_mbrs, new_mbrs;
                    curr_mbrs=new ArrayList<>(view.getMembers());
                    left_mbrs=View.leftMembers(old_view, view);
                    new_mbrs=View.newMembers(old_view, view);
                    startCacheDissemination(curr_mbrs, left_mbrs, new_mbrs); // separate task
                }
                return retval;

            case Event.BECOME_SERVER: // called after client has joined and is fully working group member
                down_prot.down(evt);
                is_server=true;
                return null;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                return down_prot.down(evt);

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                is_leaving=false;
                cluster_name=evt.getArg();
                Object ret=down_prot.down(evt);
                handleConnect();
                return ret;

            case Event.DISCONNECT:
                is_leaving=true;
                handleDisconnect();
                return down_prot.down(evt);

            default:
                return down_prot.down(evt);          // Pass on to the layer below us
        }
    }



    /* -------------------------- Private methods ---------------------------- */

    protected List<PingData> read(InputStream in) {
        List<PingData> retval=null;
        try {
            while(true) {
                try {
                    String name_str=Util.readToken(in);
                    String uuid_str=Util.readToken(in);
                    String addr_str=Util.readToken(in);
                    String coord_str=Util.readToken(in);
                    if(name_str == null || uuid_str == null || addr_str == null || coord_str == null)
                        break;

                    UUID uuid=null;
                    try {
                        long tmp=Long.valueOf(uuid_str);
                        uuid=new UUID(0, tmp);
                    }
                    catch(Throwable t) {
                        uuid=UUID.fromString(uuid_str);
                    }

                    PhysicalAddress phys_addr=new IpAddress(addr_str);
                    boolean is_coordinator=coord_str.trim().equals("T") || coord_str.trim().equals("t");

                    if(retval == null)
                        retval=new ArrayList<>();
                    retval.add(new PingData(uuid, true, name_str, phys_addr).coord(is_coordinator));
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("FailedReadingLineOfInputStream"), t);
                }
            }
            return retval;
        }
        finally {
            Util.close(in);
        }
    }

    protected void write(List<PingData> list, OutputStream out) throws Exception {
        try {
            for(PingData data: list) {
                String  logical_name=data.getLogicalName();
                Address addr=data.getAddress();
                PhysicalAddress phys_addr=data.getPhysicalAddr();
                if(logical_name == null || addr == null || phys_addr == null)
                    continue;
                out.write(logical_name.getBytes());
                out.write(WHITESPACE);

                out.write(addressAsString(addr).getBytes());
                out.write(WHITESPACE);

                out.write(phys_addr.toString().getBytes());
                out.write(WHITESPACE);

                out.write(data.isCoord()? String.format("T%n").getBytes() : String.format("F%n").getBytes());
            }
        }
        finally {
            Util.close(out);
        }
    }

    protected void addResponse(PingData rsp, boolean overwrite) {
        synchronized(ping_responses) {
            for(Iterator<Map.Entry<Long,Responses>> it=ping_responses.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Long,Responses> entry=it.next();
                long timestamp=entry.getKey();
                Responses rsps=entry.getValue();
                rsps.addResponse(rsp, overwrite);
                if(rsps.isDone() || TimeUnit.MILLISECONDS.convert(System.nanoTime() - timestamp, TimeUnit.NANOSECONDS) > discovery_rsp_expiry_time) {
                    it.remove();
                    rsps.done();
                }
            }
        }
    }

    /** Removes responses which are done or whose timeout has expired (in the latter case, an expired response is marked as done) */
    @ManagedOperation(description="Removes expired or completed responses")
    public void weedOutCompletedDiscoveryResponses() {
        synchronized(ping_responses) {
            for(Iterator<Map.Entry<Long,Responses>> it=ping_responses.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Long,Responses> entry=it.next();
                long timestamp=entry.getKey();
                Responses rsps=entry.getValue();
                if(rsps.isDone() || TimeUnit.MILLISECONDS.convert(System.nanoTime() - timestamp, TimeUnit.NANOSECONDS) > discovery_rsp_expiry_time) {
                    it.remove();
                    rsps.done();
                }
            }
        }
    }


    protected boolean addDiscoveryResponseToCaches(Address mbr, String logical_name, PhysicalAddress physical_addr) {
        if(mbr == null)
            return false;
        if(logical_name != null)
            NameCache.add(mbr, logical_name);
        if(physical_addr != null)
            return (Boolean)down(new Event(Event.ADD_PHYSICAL_ADDRESS, new Tuple<>(mbr, physical_addr)));
        return false;
    }

    protected synchronized void startCacheDissemination(List<Address> curr_mbrs, List<Address> left_mbrs, List<Address> new_mbrs) {
        timer.execute(new DiscoveryCacheDisseminationTask(curr_mbrs,left_mbrs,new_mbrs), sends_can_block);
    }


    /**
     * Creates a byte[] representation of the PingData, but DISCARDING the view it contains.
     * @param data the PingData instance to serialize.
     * @return
     */
    protected byte[] serializeWithoutView(PingData data) {
        final PingData clone = new PingData(data.getAddress(), data.isServer(), data.getLogicalName(), data.getPhysicalAddr()).coord(data.isCoord());
        try {
            return Util.streamableToByteBuffer(clone);
        }
        catch(Exception e) {
            log.error(Util.getMessage("ErrorSerializingPingData"), e);
            return null;
        }
    }

    protected static PingData deserialize(final byte[] data) throws Exception {
        return Util.streamableFromByteBuffer(PingData::new, data);
    }

    public static Buffer marshal(PingData data) {
        return Util.streamableToBuffer(data);
    }

    protected PingData readPingData(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(PingData::new, buffer, offset, length) : null;
        }
        catch(Exception ex) {
            log.error("%s: failed reading PingData from message: %s", local_addr, ex);
            return null;
        }
    }

    protected void sendDiscoveryResponse(Address logical_addr, PhysicalAddress physical_addr,
                                         String logical_name, final Address sender, boolean coord) {
        final PingData data=new PingData(logical_addr, is_server, logical_name, physical_addr).coord(coord);
        final Message rsp_msg=new Message(sender).setFlag(Message.Flag.INTERNAL, Message.Flag.OOB, Message.Flag.DONT_BUNDLE)
          .putHeader(this.id, new PingHeader(PingHeader.GET_MBRS_RSP)).setBuffer(marshal(data));

        if(stagger_timeout > 0) {
            int view_size=view != null? view.size() : 10;
            int rank=Util.getRank(view, local_addr); // returns 0 if view or local_addr are null
            long sleep_time=rank == 0? Util.random(stagger_timeout)
              : stagger_timeout * rank / view_size - (stagger_timeout / view_size);
            timer.schedule(() -> {
                log.trace("%s: received GET_MBRS_REQ from %s, sending staggered response %s", local_addr, sender, data);
                down_prot.down(rsp_msg);
            }, sleep_time, TimeUnit.MILLISECONDS, sends_can_block);
            return;
        }

        log.trace("%s: received GET_MBRS_REQ from %s, sending response %s", local_addr, sender, data);
        down_prot.down(rsp_msg);
    }

    protected static String addressAsString(Address address) {
        if(address == null)
            return "";
        if(address instanceof UUID)
            return ((UUID) address).toStringLong();
        return address.toString();
    }

    protected boolean isCoord(Address member) {return member.equals(current_coord);}

    /** Disseminates cache information (UUID/IP adddress/port/name) to the given members
     * @param current_mbrs The current members. Guaranteed to be non-null. This is a copy and can be modified.
     * @param left_mbrs The members which left. These are excluded from dissemination. Can be null if no members left
     * @param new_mbrs The new members that we need to disseminate the information to. Will be all members if null.
     */
    protected void disseminateDiscoveryInformation(List current_mbrs, List<Address> left_mbrs, List<Address> new_mbrs) {
        if(new_mbrs == null || new_mbrs.isEmpty())
            return;

        if(local_addr != null)
            current_mbrs.remove(local_addr);
        if(left_mbrs != null)
            current_mbrs.removeAll(left_mbrs);

        // 1. Send information about <everyone - self - left_mbrs> to new_mbrs
        Set<Address> info=new HashSet<>(current_mbrs);
        for(Address addr : info) {
            PhysicalAddress phys_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS,addr));
            if(phys_addr == null)
                continue;
            boolean is_coordinator=isCoord(addr);
            for(Address target : new_mbrs)
                sendDiscoveryResponse(addr,phys_addr,NameCache.get(addr),target,is_coordinator);
        }

        // 2. Send information about new_mbrs to <everyone - self - left_mbrs - new_mbrs>
        Set<Address> targets=new HashSet<>(current_mbrs);
        targets.removeAll(new_mbrs);

        if(!targets.isEmpty()) {
            for(Address addr : new_mbrs) {
                PhysicalAddress phys_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS,addr));
                if(phys_addr == null)
                    continue;
                boolean is_coordinator=isCoord(addr);
                for(Address target : targets)
                    sendDiscoveryResponse(addr,phys_addr,NameCache.get(addr),target,is_coordinator);
            }
        }
    }


    protected class DiscoveryCacheDisseminationTask implements Runnable {
        protected final List<Address> curr_mbrs, left_mbrs, new_mbrs;

        public DiscoveryCacheDisseminationTask(List<Address> curr_mbrs,List<Address> left_mbrs,List<Address> new_mbrs) {
            this.curr_mbrs=curr_mbrs;
            this.left_mbrs=left_mbrs;
            this.new_mbrs=new_mbrs;
        }

        public void run() {
            disseminateDiscoveryInformation(curr_mbrs, left_mbrs, new_mbrs);
        }
    }

}
