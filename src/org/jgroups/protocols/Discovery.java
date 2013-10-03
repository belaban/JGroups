package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * The Discovery protocol retrieves the initial membership (used by GMS and MERGE2) by sending discovery requests.
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

    @Property(description="Timeout to wait for the initial members")
    protected long    timeout=3000;

    @Property(description="Minimum number of initial members to get a response from")
    protected int     num_initial_members=10;

    @Deprecated
    @Property(description="Minimum number of server responses (PingData.isServer()=true). If this value is " +
            "greater than 0, we'll ignore num_initial_members",deprecatedMessage="not used anymore")
    protected int     num_initial_srv_members=0;

    @Property(description="Return from the discovery phase as soon as we have 1 coordinator response")
    protected boolean break_on_coord_rsp=true;

    @Property(description="Whether or not to return the entire logical-physical address cache mappings on a " +
      "discovery request, or not.")
    protected boolean return_entire_cache=false;

    @Property(description="If greater than 0, we'll wait a random number of milliseconds in range [0..stagger_timeout] " +
      "before sending a discovery response. This prevents traffic spikes in large clusters when everyone sends their " +
      "discovery response at the same time")
    protected long    stagger_timeout=0;

    @Property(description="Always sends a discovery response, no matter what",writable=true)
    protected boolean force_sending_discovery_rsps=true;


    @Property(description="If a persistent disk cache (PDC) is present, combine the discovery results with the " +
      "contents of the disk cache before returning the results")
    protected boolean use_disk_cache=false;

    @Property(description="When sending a discovery request, always send the physical address and logical name too")
    protected boolean always_send_physical_addr_with_discovery_request=false;


    @ManagedOperation(description="Sets force_sending_discovery_rsps")
    public void setForceSendingDiscoveryRsps(boolean flag) {
        force_sending_discovery_rsps=flag;
    }

    /* ---------------------------------------------   JMX      ------------------------------------------------------ */


    @ManagedAttribute(description="Total number of discovery requests sent ")
    protected int num_discovery_requests=0;

    /** The largest cluster size found so far (gets reset on stop()) */
    @ManagedAttribute
    private int max_found_members=0;


    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected volatile boolean      is_server=false;
    protected volatile boolean      is_leaving=false;
    protected TimeScheduler         timer=null;
    protected View                  view;
    protected final List<Address>   members=new ArrayList<Address>(11);
    @ManagedAttribute(description="Whether this member is the current coordinator")
    protected boolean               is_coord;
    protected Address               local_addr=null;
    protected Address               current_coord;
    protected String                group_addr;
    protected final Set<Responses>  ping_responses=new HashSet<Responses>();



    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved from protocol stack");
        if(stagger_timeout < 0)
            throw new IllegalArgumentException("stagger_timeout cannot be negative");
        if(stagger_timeout > timeout) {
            log.debug("stagger_timeout (" + stagger_timeout + ") was greater than timeout (" + timeout +
                        "); setting it to " + timeout + " ms");
            stagger_timeout=timeout;
        }
    }

    /**
     * Grab all current cluster members
     * @return A list of the cluster members (usually IpAddresses), or null if the transport is multicast-enabled.
     *         Returns an empty list if no cluster members could be found.
     * @param cluster_name
     */
    public abstract Collection<PhysicalAddress> fetchClusterMembers(String cluster_name);

    /** Whether or not to send each discovery request on a separate (timer) thread. If disabled,
     * a discovery request will be sent to all members fetched by {@link #fetchClusterMembers(String)} sequentially */
    public abstract boolean sendDiscoveryRequestsInParallel();


    public abstract boolean isDynamic();


    public void handleDisconnect() {
    }

    public void handleConnect() {
    }

    public void discoveryRequestReceived(Address sender, String logical_name, Collection<PhysicalAddress> physical_addrs) {

    }

    public long      getTimeout()                       {return timeout;}
    public void      setTimeout(long timeout)           {this.timeout=timeout;}
    public int       getNumInitialMembers()             {return num_initial_members;}
    public void      setNumInitialMembers(int num)      {this.num_initial_members=num;}
    public int       getNumberOfDiscoveryRequestsSent() {return num_discovery_requests;}
    public long      timeout()                          {return timeout;}
    public Discovery timeout(long timeout)              {this.timeout=timeout; return this;}
    public long      numInitialMembers()                {return num_initial_members;}
    public Discovery numInitialMembers(int num)         {this.num_initial_members=num; return this;}
    public boolean   breakOnCoordResponse()             {return break_on_coord_rsp;}
    public Discovery breakOnCoordResponse(boolean flag) {break_on_coord_rsp=flag; return this;}
    public boolean   returnEntireCache()                {return return_entire_cache;}
    public Discovery returnEntireCache(boolean flag)    {return_entire_cache=flag; return this;}
    public long      staggerTimeout()                   {return stagger_timeout;}
    public Discovery staggerTimeout(long timeout)       {stagger_timeout=timeout; return this;}
    public boolean   forceDiscoveryResponses()          {return force_sending_discovery_rsps;}
    public Discovery forceDiscoveryResponses(boolean f) {force_sending_discovery_rsps=f; return this;}
    public boolean   useDiskCache()                     {return use_disk_cache;}
    public Discovery useDiskCache(boolean flag)         {use_disk_cache=flag; return this;}




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

    public List<Integer> providedUpServices() {
        List<Integer> ret=new ArrayList<Integer>(3);
        ret.add(Event.FIND_INITIAL_MBRS);
        ret.add(Event.FIND_ALL_VIEWS);
        ret.add(Event.GET_PHYSICAL_ADDRESS);
        return ret;
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
        max_found_members=0;
    }


    /**
     * Finds initial members
     * @param promise
     * @return
     */
    public List<PingData> findInitialMembers(Promise<JoinRsp> promise) {
        return findMembers(promise, num_initial_members, break_on_coord_rsp, null);
    }

    public List<PingData> findAllViews(Promise<JoinRsp> promise) {
        int num_expected_mbrs=Math.max(max_found_members, Math.max(num_initial_members, view != null? view.size() : num_initial_members));
        max_found_members=Math.max(max_found_members, num_expected_mbrs);
        return findMembers(promise, num_expected_mbrs, false, getViewId());
    }

    protected List<PingData> findMembers(Promise<JoinRsp> promise, int num_expected_rsps,
                                         boolean break_on_coord, ViewId view_id) {
        num_discovery_requests++;

        final Responses rsps=new Responses(num_expected_rsps, break_on_coord, promise);
        synchronized(ping_responses) {
            ping_responses.add(rsps);
        }

        try {
            sendDiscoveryRequest(group_addr, promise, view_id);
        }
        catch(InterruptedIOException ie) {
            ;
        }
        catch(InterruptedException ex) {
            ;
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled())
                log.error("failed sending discovery request", ex);
        }

        try {
            return rsps.get(timeout);
        }
        catch(Exception e) {
            return new LinkedList<PingData>();
        }
        finally {
            synchronized(ping_responses) {
                ping_responses.remove(rsps);
            }
        }
    }

    public void sendDiscoveryRequest(String cluster_name, Promise promise, ViewId view_id) throws Exception {
        PingData data=null;
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));

        // https://issues.jboss.org/browse/JGRP-1670
        if(view_id == null || always_send_physical_addr_with_discovery_request)
            data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addr != null? Arrays.asList(physical_addr) : null);

        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name).viewId(view_id);

        Collection<PhysicalAddress> cluster_members=fetchClusterMembers(cluster_name);
        if(cluster_members == null) {
            // message needs to have DONT_BUNDLE flag: if A sends message M to B, and we need to fetch B's physical
            // address, then the bundler thread blocks until the discovery request has returned. However, we cannot send
            // the discovery *request* until the bundler thread has returned from sending M
            Message msg=new Message(null).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
              .putHeader(getId(), hdr).setBuffer(marshal(data));
            sendMcastDiscoveryRequest(msg);
        }
        else {
            if(use_disk_cache) {
                // this only makes sense if we have PDC below us
                Collection<PhysicalAddress> list=(Collection<PhysicalAddress>)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESSES));
                if(list != null)
                    for(PhysicalAddress phys_addr: list)
                        if(!cluster_members.contains(phys_addr))
                            cluster_members.add(phys_addr);
            }

            if(cluster_members.isEmpty()) { // if we don't find any members, return immediately
                if(promise != null)
                    promise.setResult(null);
            }
            else {
                for(final Address addr: cluster_members) {
                    if(physical_addr != null && addr.equals(physical_addr)) // no need to send the request to myself
                        continue;
                    // the message needs to be DONT_BUNDLE, see explanation above
                    final Message msg=new Message(addr).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
                      .putHeader(this.id, hdr).setBuffer(marshal(data));
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": sending discovery request to " + msg.getDest());
                    if(!sendDiscoveryRequestsInParallel()) {
                        down_prot.down(new Event(Event.MSG, msg));
                    }
                    else {
                        timer.execute(new Runnable() {
                            public void run() {
                                try {
                                    down_prot.down(new Event(Event.MSG, msg));
                                }
                                catch(Exception ex){
                                    if(log.isErrorEnabled())
                                        log.error(local_addr + ": failed sending discovery request to " + addr + ": " +  ex);
                                }
                            }
                        });
                    }
                }
            }
        }
    }

    protected void sendMcastDiscoveryRequest(Message discovery_request) {
        down_prot.down(new Event(Event.MSG, discovery_request));
    }



    @ManagedOperation(description="Runs the discovery protocol to find initial members")
    public String findInitialMembersAsString() {
      List<PingData> results=findInitialMembers(null);
        if(results == null || results.isEmpty()) return "<empty>";
        StringBuilder sb=new StringBuilder();
        for(PingData rsp: results) {
            sb.append(rsp).append("\n");
        }
        return sb.toString();
    }


    @ManagedOperation(description="Runs the discovery protocol to find all views")
    public String findAllViewsAsString() {
        List<PingData> rsps=findAllViews(null);
        if(rsps == null || rsps.isEmpty()) return "<empty>";
        StringBuilder sb=new StringBuilder();
        for(PingData data: rsps) {
            View v=data.getView();
            if(v !=  null)
                sb.append(v).append("\n");
        }
        return sb.toString();
    }


    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>PassDown</code> or c) the event (or another event) is sent up
     * the stack using <code>PassUp</code>.
     * <p/>
     * For the PING protocol, the Up operation does the following things.
     * 1. If the event is a Event.MSG then PING will inspect the message header.
     * If the header is null, PING simply passes up the event
     * If the header is PingHeader.GET_MBRS_REQ then the PING protocol
     * will PassDown a PingRequest message
     * If the header is PingHeader.GET_MBRS_RSP we will add the message to the initial members
     * vector and wake up any waiting threads.
     * 2. If the event is Event.SET_LOCAL_ADDR we will simple set the local address of this protocol
     * 3. For all other messages we simple pass it up to the protocol above
     *
     * @param evt - the event that has been sent from the layer below
     */

    @SuppressWarnings("unchecked")
    public Object up(Event evt) {

        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                PingHeader hdr=(PingHeader)msg.getHeader(this.id);
                if(hdr == null)
                    return up_prot.up(evt);

                if(is_leaving)
                    return null; // prevents merging back a leaving member (https://issues.jboss.org/browse/JGRP-1336)

                PingData data=readPingData(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                Address logical_addr=data != null? data.getAddress() : null;

                switch(hdr.type) {

                    case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
                        if(group_addr == null || hdr.cluster_name == null) {
                            if(log.isWarnEnabled())
                                log.warn("group_addr (" + group_addr + ") or cluster_name of header (" + hdr.cluster_name
                                        + ") is null; passing up discovery request from " + msg.getSrc() + ", but this should not" +
                                        " be the case");
                        }
                        else {
                            if(!group_addr.equals(hdr.cluster_name)) {
                                if(log.isWarnEnabled())
                                    log.warn(local_addr + ": discarding discovery request for cluster '" + hdr.cluster_name + "' from " +
                                            msg.getSrc() + "; our cluster name is '" + group_addr + "'. " +
                                            "Please separate your clusters cleanly.");
                                return null;
                            }
                        }

                        // add physical address and logical name of the discovery sender (if available) to the cache
                        if(data != null) {
                            if(logical_addr == null)
                                logical_addr=msg.getSrc();
                            Collection<PhysicalAddress> physical_addrs=data.getPhysicalAddrs();
                            PhysicalAddress physical_addr=physical_addrs != null && !physical_addrs.isEmpty()? physical_addrs.iterator().next() : null;
                            if(logical_addr != null && data.getLogicalName() != null)
                                UUID.add(logical_addr, data.getLogicalName());
                            if(logical_addr != null && physical_addr != null)
                                down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address,PhysicalAddress>(logical_addr, physical_addr)));
                            discoveryRequestReceived(msg.getSrc(), data.getLogicalName(), physical_addrs);

                            synchronized(ping_responses) {
                                for(Responses response: ping_responses) {
                                    response.addResponse(data, false);
                                }
                            }
                        }

                        if(hdr.view_id != null) {
                            // If the discovery request is merge-triggered, and the ViewId shipped with it
                            // is the same as ours, we don't respond (JGRP-1315).
                            ViewId my_view_id=view != null? view.getViewId() : null;
                            if(my_view_id != null && my_view_id.equals(hdr.view_id))
                                return null;

                            boolean send_discovery_rsp=force_sending_discovery_rsps || is_coord
                              || current_coord == null || current_coord.equals(msg.getSrc());
                            if(!send_discovery_rsp) {
                                if(log.isTraceEnabled())
                                    log.trace(local_addr + ": suppressing discovery response as I'm not a coordinator and the " +
                                                "discovery request was not sent by a coordinator");
                                return null;
                            }
                            if(isMergeRunning()) {
                                if(log.isTraceEnabled())
                                    log.trace(local_addr + ": suppressing discovery response as a merge is in progress");
                                return null;
                            }
                        }


                        if(return_entire_cache) {
                            Map<Address,PhysicalAddress> cache=(Map<Address,PhysicalAddress>)down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS));
                            if(cache != null) {
                                for(Map.Entry<Address,PhysicalAddress> entry: cache.entrySet()) {
                                    Address addr=entry.getKey();
                                    // JGRP-1492: only return our own address, and addresses in view.
                                    if (addr.equals(local_addr) || members.contains(addr)) {
                                        PhysicalAddress physical_addr=entry.getValue();
                                        sendDiscoveryResponse(addr, Arrays.asList(physical_addr), is_server,
                                                              hdr.view_id != null, UUID.get(addr), msg.getSrc());
                                    }
                                }
                            }
                        }
                        else {
                            List<PhysicalAddress> physical_addrs=hdr.view_id != null? null :
                              Arrays.asList((PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)));
                            sendDiscoveryResponse(local_addr, physical_addrs, is_server, hdr.view_id != null,
                                                  UUID.get(local_addr), msg.getSrc());
                        }
                        return null;

                    case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                        // add physical address (if available) to transport's cache
                        if(data != null) {
                            Address response_sender=msg.getSrc();
                            if(logical_addr == null)
                                logical_addr=msg.getSrc();
                            Collection<PhysicalAddress> addrs=data.getPhysicalAddrs();
                            PhysicalAddress physical_addr=addrs != null && !addrs.isEmpty()?
                                    addrs.iterator().next() : null;
                            if(logical_addr != null && data.getLogicalName() != null)
                                UUID.add(logical_addr, data.getLogicalName());
                            if(logical_addr != null && physical_addr != null)
                                down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address,PhysicalAddress>(logical_addr, physical_addr)));

                            if(log.isTraceEnabled())
                                log.trace(local_addr + ": received GET_MBRS_RSP from " + response_sender + ": " + data);
                            boolean overwrite=logical_addr != null && logical_addr.equals(response_sender);
                            synchronized(ping_responses) {
                                for(Responses response: ping_responses) {
                                    response.addResponse(data, overwrite);
                                }
                            }
                        }
                        return null;

                    default:
                        if(log.isWarnEnabled()) log.warn("got PING header with unknown type (" + hdr.type + ')');
                        return null;
                }


            case Event.GET_PHYSICAL_ADDRESS:
                try {
                    sendDiscoveryRequest(group_addr, null, null);
                }
                catch(InterruptedIOException ie) {
                    if(log.isWarnEnabled()){
                        log.warn("Discovery request for cluster " + group_addr + " interrupted");
                    }
                    Thread.currentThread().interrupt();
                }
                catch(Exception ex) {
                    if(log.isErrorEnabled())
                        log.error("failed sending discovery request", ex);
                }
                return null;


            case Event.FIND_INITIAL_MBRS:      // sent by transport
                return findInitialMembers(null);
        }

        return up_prot.up(evt);
    }



    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>PassDown</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>up_prot.up()</code>.
     * The PING protocol is interested in several different down events,
     * Event.FIND_INITIAL_MBRS - sent by the GMS layer and expecting a GET_MBRS_OK
     * Event.TMP_VIEW and Event.VIEW_CHANGE - a view change event
     * Event.BECOME_SERVER - called after client has joined and is fully working group member
     * Event.CONNECT, Event.DISCONNECT.
     */
    @SuppressWarnings("unchecked")
    public Object down(Event evt) {

        switch(evt.getType()) {

            case Event.FIND_INITIAL_MBRS:      // sent by GMS layer
            case Event.FIND_ALL_VIEWS:
                // sends the GET_MBRS_REQ to all members, waits 'timeout' ms or until 'num_initial_members' have been retrieved
                long start=System.currentTimeMillis();
                boolean find_all_views=evt.getType() == Event.FIND_ALL_VIEWS;
                Promise<JoinRsp> promise=(Promise<JoinRsp>)evt.getArg();
                List<PingData> rsps=find_all_views? findAllViews(promise) : findInitialMembers(promise);
                long diff=System.currentTimeMillis() - start;
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": discovery took "+ diff + " ms: responses: " + Util.printPingData(rsps));
                return rsps;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                List<Address> tmp;
                view=(View)evt.getArg();
                if((tmp=view.getMembers()) != null) {
                    synchronized(members) {
                        members.clear();
                        members.addAll(tmp);
                    }
                }
                current_coord=!members.isEmpty()? members.get(0) : null;
                is_coord=current_coord != null && local_addr != null && current_coord.equals(local_addr);

                return down_prot.down(evt);

            case Event.BECOME_SERVER: // called after client has joined and is fully working group member
                down_prot.down(evt);
                is_server=true;
                return null;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                return down_prot.down(evt);

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                is_leaving=false;
                group_addr=(String)evt.getArg();
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



    /**
     * Creates a byte[] representation of the PingData, but DISCARDING the view it contains.
     * @param data the PingData instance to serialize.
     * @return
     */
    protected byte[] serializeWithoutView(PingData data) {
        final PingData clone = new PingData(data.getAddress(), null, data.isServer(), data.getLogicalName(),  data.getPhysicalAddrs());
        try {
            return Util.streamableToByteBuffer(clone);
        }
        catch(Exception e) {
            log.error("Error", e);
            return null;
        }
    }

    protected PingData deserialize(final byte[] data) {
        try {
            return (PingData)Util.streamableFromByteBuffer(PingData.class, data);
        }
        catch(Exception e) {
            log.error("Error", e);
            return null;
        }
    }

    public static Buffer marshal(PingData data) {
        return Util.streamableToBuffer(data);
    }

    protected PingData readPingData(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(PingData.class, buffer, offset, length) : null;
        }
        catch(Exception ex) {
            log.error("%s: failed reading PingData from message: %s", local_addr, ex);
            return null;
        }
    }

    protected void sendDiscoveryResponse(Address logical_addr, List<PhysicalAddress> physical_addrs,
                                         boolean is_server, boolean return_view_only, String logical_name, final Address sender) {
        final PingData data=return_view_only? new PingData(logical_addr, view, is_server, null, null)
          : new PingData(logical_addr, null, view != null? view.getViewId() : null, is_server, logical_name, physical_addrs);

        final Message rsp_msg=new Message(sender).setFlag(Message.Flag.INTERNAL)
          .putHeader(this.id, new PingHeader(PingHeader.GET_MBRS_RSP)).setBuffer(marshal(data));

        if(stagger_timeout > 0) {
            int view_size=view != null? view.size() : 10;
            int rank=Util.getRank(view, local_addr); // returns 0 if view or local_addr are null
            long sleep_time=rank == 0? Util.random(stagger_timeout)
              : stagger_timeout * rank / view_size - (stagger_timeout / view_size);
            timer.schedule(new Runnable() {
                public void run() {
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": received GET_MBRS_REQ from " + sender + ", sending staggered response " + data);
                    down_prot.down(new Event(Event.MSG, rsp_msg));
                }
            }, sleep_time, TimeUnit.MILLISECONDS);
            return;
        }

        if(log.isTraceEnabled())
            log.trace(local_addr + ": received GET_MBRS_REQ from " + sender + ", sending response " + data);
        down_prot.down(new Event(Event.MSG, rsp_msg));
    }



    protected static class Responses {
        final Promise<JoinRsp>  promise;
        final List<PingData>    ping_rsps=new ArrayList<PingData>();
        final int               num_expected_rsps;
        final boolean           break_on_coord_rsp;

        protected Responses(int num_expected_rsps, boolean break_on_coord_rsp, Promise<JoinRsp> promise) {
            this.num_expected_rsps=num_expected_rsps;
            this.break_on_coord_rsp=break_on_coord_rsp;
            this.promise=promise != null? promise : new Promise<JoinRsp>();
        }

        public void addResponse(PingData rsp) {
            addResponse(rsp, false);
        }

        public void addResponse(PingData rsp, boolean overwrite) {
            if(rsp == null)
                return;
            promise.getLock().lock();
            try {
                if(overwrite)
                    ping_rsps.remove(rsp);

                // https://jira.jboss.org/jira/browse/JGRP-1179
                int index=ping_rsps.indexOf(rsp);
                if(index == -1) {
                    ping_rsps.add(rsp);
                    promise.getCond().signalAll();
                }
                else if(rsp.isCoord()) {
                    PingData pr=ping_rsps.get(index);

                    // Check if the already existing element is not server
                    if(!pr.isCoord()) {
                        ping_rsps.set(index, rsp);
                        promise.getCond().signalAll();
                    }
                }
            }
            finally {
                promise.getLock().unlock();
            }
        }

        public List<PingData> get(long timeout) throws InterruptedException{
            long start_time=System.currentTimeMillis(), time_to_wait=timeout;

            promise.getLock().lock();
            try {
                while(time_to_wait > 0 && !promise.hasResult()) {
                    if(ping_rsps.size() >= num_expected_rsps && (break_on_coord_rsp && containsCoordinatorResponse(ping_rsps)))
                        return new LinkedList<PingData>(ping_rsps);

                    if(break_on_coord_rsp &&  containsCoordinatorResponse(ping_rsps))
                        return new LinkedList<PingData>(ping_rsps);

                    promise.getCond().await(time_to_wait, TimeUnit.MILLISECONDS);
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }
                return new LinkedList<PingData>(ping_rsps);
            }
            finally {
                promise.getLock().unlock();
            }
        }


        private static boolean containsCoordinatorResponse(Collection<PingData> rsps) {
            if(rsps == null || rsps.isEmpty())
                return false;
            for(PingData rsp: rsps) {
                if(rsp.isCoord())
                    return true;
            }
            return false;
        }

    }
}