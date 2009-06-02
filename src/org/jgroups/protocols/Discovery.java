package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * The Discovery protocol layer retrieves the initial membership (used by the
 * GMS when started by sending event FIND_INITIAL_MBRS down the stack). We do
 * this by specific subclasses, e.g. by mcasting PING requests to an IP MCAST
 * address or, if gossiping is enabled, by contacting the GossipRouter. The
 * responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group. When we are a server (after
 * having received the BECOME_SERVER event), we'll respond to PING requests with
 * a PING response.
 * <p>
 * The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack. The following properties are
 * available
 * <ul>
 * <li>timeout - the timeout (ms) to wait for the initial members, default is
 * 3000=3 secs
 * <li>num_initial_members - the minimum number of initial members for a
 * FIND_INITAL_MBRS, default is 2
 * <li>num_ping_requests - the number of GET_MBRS_REQ messages to be sent
 * (min=1), distributed over timeout ms
 * </ul>
 * 
 * @author Bela Ban
 * @version $Id: Discovery.java,v 1.58 2009/06/02 13:53:36 belaban Exp $
 */
@MBean
public abstract class Discovery extends Protocol {   
    
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Timeout to wait for the initial members. Default is 3000 msec")
    @ManagedAttribute(description="Timeout (ms) to wait for the initial members", writable=true)
    long timeout=3000;

    @Property(description="Minimum number of initial members to get a response from. Default is 2")
    @ManagedAttribute(description="Minimum number of initial members to get a response from", writable=true)
    int num_initial_members=2;

    @Property(description="Minimum number of server responses (PingData.isServer()=true). If this value is " +
            "greater than 0, we'll ignore num_initial_members")
    @ManagedAttribute(writable=true, description="Minimum number of server responses (PingData.isServer()=true). " +
            "If this value is greater than 0, we'll ignore num_initial_members")
    int num_initial_srv_members=0;

    @Property(description="Return from the discovery phase as soon as we have 1 coordinator response")
    @ManagedAttribute(writable=true, description="Return from the discovery phase as soon as we have 1 coordinator response")
    boolean break_on_coord_rsp=true;

    @Property(description="Number of discovery requests to be sent distributed over timeout. Default is 2")
    @ManagedAttribute(description="Number of discovery requests to be sent (min=1), " + "distributed over timeout ms", writable=true)
    int num_ping_requests=2;

    @Property(description="Whether or not to return the entire logical-physical address cache mappings on a " +
            "discovery request, or not. Default is false, except for TCPPING")
    boolean return_entire_cache=false;

    
    /* ---------------------------------------------   JMX      ------------------------------------------------------ */

    
    @ManagedAttribute(description="Total number of discovery requests sent ")
    int num_discovery_requests=0;


    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    
    private volatile boolean is_server=false;
    protected TimeScheduler timer=null;

    protected final Vector<Address> members=new Vector<Address>(11);
    protected Address local_addr=null;
    protected String group_addr=null;
    protected final Set<Responses> ping_responses=new HashSet<Responses>();
    private final PingSenderTask sender=new PingSenderTask();
    

    
    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved from protocol stack");
    }


    public abstract void sendGetMembersRequest(String cluster_name, Promise promise) throws Exception;


    public void handleDisconnect() {
    }

    public void handleConnect() {
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout=timeout;        
    }

    public int getNumInitialMembers() {
        return num_initial_members;
    }

    public void setNumInitialMembers(int num_initial_members) {
        this.num_initial_members=num_initial_members;        
    }

    public int getNumPingRequests() {
        return num_ping_requests;
    }

    public void setNumPingRequests(int num_ping_requests) {
        this.num_ping_requests=num_ping_requests;
    }

    public int getNumberOfDiscoveryRequestsSent() {
        return num_discovery_requests;
    }


    public Vector<Integer> providedUpServices() {
        Vector<Integer> ret=new Vector<Integer>(1);
        ret.addElement(new Integer(Event.FIND_INITIAL_MBRS));
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
    }

    /**
     * Finds the initial membership: sends a GET_MBRS_REQ to all members, waits 'timeout' ms or
     * until 'num_initial_members' have been retrieved
     * @return List<PingData>
     */
    public List<PingData> findInitialMembers(Promise<JoinRsp> promise) {
        num_discovery_requests++;

        final Responses rsps=new Responses(num_initial_members, num_initial_srv_members, break_on_coord_rsp, promise);
        synchronized(ping_responses) {
            ping_responses.add(rsps);
        }

        sender.start(group_addr, promise);
        try {
            return rsps.get(timeout);
        }
        catch(Exception e) {
            return new LinkedList<PingData>();
        }
        finally {
        	sender.stop();
            synchronized(ping_responses) {
                ping_responses.remove(rsps);
            }
        }
    }


    public String findInitialMembersAsString() {
    	List<PingData> results=findInitialMembers(null);
        if(results == null || results.isEmpty()) return "<empty>";
        StringBuilder sb=new StringBuilder();
        for(PingData rsp: results) {
            sb.append(rsp).append("\n");
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

    public Object up(Event evt) {
        Message msg;
        PingData rsp;
        Address coord;

        switch(evt.getType()) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            PingHeader hdr=(PingHeader)msg.getHeader(getName());
            if(hdr == null)
                return up_prot.up(evt);

            switch(hdr.type) {

            case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
                if(local_addr != null && msg.getSrc() != null && local_addr.equals(msg.getSrc())) {
                    return null;
                }

                if(group_addr == null || hdr.cluster_name == null) {
                    if(log.isWarnEnabled())
                        log.warn("group_addr (" + group_addr + ") or cluster_name of header (" + hdr.cluster_name
                        + ") is null; passing up discovery request from " + msg.getSrc() + ", but this should not" +
                                " be the case");
                }
                else {
                    if(!group_addr.equals(hdr.cluster_name)) {
                        if(log.isWarnEnabled())
                            log.warn("discarding discovery request for cluster '" + hdr.cluster_name + "' from " +
                                    msg.getSrc() + "; our cluster name is '" + group_addr + "'. " +
                                    "Please separate your clusters cleanly.");
                        return null;
                    }
                }

                // add physical address (if available) to transport's cache
                if(hdr.arg != null) {
                    Address logical_addr=hdr.arg.getAddress();
                    if(logical_addr == null)
                        logical_addr=msg.getSrc();
                    List<PhysicalAddress> physical_addrs=hdr.arg.getPhysicalAddrs();
                    PhysicalAddress physical_addr=physical_addrs != null && !physical_addrs.isEmpty()? physical_addrs.get(0) : null;
                    if(logical_addr != null && physical_addr != null)
                        down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address,PhysicalAddress>(logical_addr, physical_addr)));
                    if(logical_addr != null && hdr.arg.getLogicalName() != null)
                        UUID.add((UUID)logical_addr, hdr.arg.getLogicalName());
                }

                synchronized(members) {
                    coord=!members.isEmpty()? members.firstElement() : local_addr;
                }

                if(return_entire_cache) {
                    Map<Address,PhysicalAddress> cache=(Map<Address,PhysicalAddress>)down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS));
                    if(cache != null) {
                        for(Map.Entry<Address,PhysicalAddress> entry: cache.entrySet()) {
                            Address logical_addr=entry.getKey();
                            PhysicalAddress physical_addr=entry.getValue();
                            sendDiscoveryResponse(logical_addr, Arrays.asList(physical_addr), coord, is_server,
                                                  UUID.get(logical_addr), msg.getSrc());
                        }
                    }
                }
                else {
                    PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
                    sendDiscoveryResponse(local_addr, Arrays.asList(physical_addr), coord, is_server,
                                          UUID.get(local_addr), msg.getSrc());
                }
                return null;

            case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                rsp=hdr.arg;

                if(log.isTraceEnabled())
                    log.trace("received GET_MBRS_RSP, rsp=" + rsp);

                // add physical address (if available) to transport's cache
                if(rsp != null) {
                    Address logical_addr=rsp.getAddress();
                    if(logical_addr == null)
                        logical_addr=msg.getSrc();
                    List<PhysicalAddress> physical_addrs=rsp.getPhysicalAddrs();
                    PhysicalAddress physical_addr=physical_addrs != null && !physical_addrs.isEmpty()?
                            physical_addrs.get(0) : null;
                    if(logical_addr != null && physical_addr != null)
                        down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address,PhysicalAddress>(logical_addr, physical_addr)));
                    if(logical_addr != null && rsp.getLogicalName() != null)
                        UUID.add((UUID)logical_addr, rsp.getLogicalName());
                }
                
                synchronized(ping_responses) {
                    for(Responses rsps: ping_responses)
                        rsps.addResponse(rsp);
                }
                return null;

            default:
                if(log.isWarnEnabled()) log.warn("got PING header with unknown type (" + hdr.type + ')');
                return null;
            }


        case Event.GET_PHYSICAL_ADDRESS:
            try {
                sendGetMembersRequest(group_addr, null);
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
    public Object down(Event evt) {

        switch(evt.getType()) {

        case Event.FIND_INITIAL_MBRS:   // sent by GMS layer, pass up a GET_MBRS_OK event
            // sends the GET_MBRS_REQ to all members, waits 'timeout' ms or until 'num_initial_members' have been retrieved
            long start=System.currentTimeMillis();
            List<PingData> rsps=findInitialMembers((Promise<JoinRsp>)evt.getArg());
            long diff=System.currentTimeMillis() - start;
            if(log.isTraceEnabled())
                log.trace("discovery took "+ diff + " ms: responses: " + Util.printPingData(rsps));
            return rsps;

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            Vector<Address> tmp;
            if((tmp=((View)evt.getArg()).getMembers()) != null) {
                synchronized(members) {
                    members.clear();
                    members.addAll(tmp);
                }
            }
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
            group_addr=(String)evt.getArg();
            Object ret=down_prot.down(evt);
            handleConnect();
            return ret;

        case Event.DISCONNECT:
            handleDisconnect();
            return down_prot.down(evt);

        default:
            return down_prot.down(evt);          // Pass on to the layer below us
        }
    }



    /* -------------------------- Private methods ---------------------------- */


    @SuppressWarnings("unchecked")
	protected final View makeView(Vector mbrs) {
        Address coord;
        long id;
        ViewId view_id=new ViewId(local_addr);

        coord=view_id.getCoordAddress();
        id=view_id.getId();
        return new View(coord, id, mbrs);
    }


    private void sendDiscoveryResponse(Address logical_addr, List<PhysicalAddress> physical_addrs,
                                       Address coord, boolean is_server, String logical_name, Address sender) {
        PingData ping_rsp=new PingData(logical_addr, coord, is_server, logical_name, physical_addrs);
        Message rsp_msg=new Message(sender, null, null);
        rsp_msg.setFlag(Message.OOB);
        PingHeader rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
        rsp_msg.putHeader(getName(), rsp_hdr);
        if(log.isTraceEnabled())
            log.trace("received GET_MBRS_REQ from " + sender + ", sending response " + rsp_hdr);
        down_prot.down(new Event(Event.MSG, rsp_msg));
    }
    

    class PingSenderTask {        
        private Future<?>      senderFuture;

        public PingSenderTask() {}

        public synchronized void start(final String cluster_name, final Promise promise) {
            long delay = (long)(timeout / (double)num_ping_requests);
            if(senderFuture == null || senderFuture.isDone()) {
                senderFuture=timer.scheduleWithFixedDelay(new Runnable() {
                    public void run() {
                        try {
                            sendGetMembersRequest(cluster_name, promise);
                        }
                        catch(InterruptedIOException ie) {
                            if(log.isWarnEnabled()){
                                log.warn("Discovery request for cluster " + cluster_name + " interrupted");
                            }
                            Thread.currentThread().interrupt();
                        }
                        catch(Exception ex) {
                            if(log.isErrorEnabled())
                                log.error("failed sending discovery request", ex);
                        }
                    }
                }, 0, delay, TimeUnit.MILLISECONDS);
            }
        }

        public synchronized void stop() {
            if(senderFuture != null) {
                senderFuture.cancel(true);
                senderFuture=null;
            }
        }      
    }


    protected static class Responses {
        final Promise<JoinRsp>  promise;
        final List<PingData>    ping_rsps=new LinkedList<PingData>();
        final int               num_expected_rsps;
        final int               num_expected_srv_rsps;
        final boolean           break_on_coord_rsp;

        public Responses(int num_expected_rsps, int num_expected_srv_rsps, boolean break_on_coord_rsp, Promise<JoinRsp> promise) {
            this.num_expected_rsps=num_expected_rsps;
            this.num_expected_srv_rsps=num_expected_srv_rsps;
            this.break_on_coord_rsp=break_on_coord_rsp;
            this.promise=promise != null? promise : new Promise<JoinRsp>();
        }

        public void addResponse(PingData rsp) {
            if(rsp == null)
                return;
            promise.getLock().lock();
            try {
                if(!ping_rsps.contains(rsp)) {
                    ping_rsps.add(rsp);
                    promise.getCond().signalAll();
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
                    // if num_expected_srv_rsps > 0, then it overrides num_expected_rsps
                    if(num_expected_srv_rsps > 0) {
                        int received_srv_rsps=getNumServerResponses(ping_rsps);
                        if(received_srv_rsps >= num_expected_srv_rsps)
                            return new LinkedList<PingData>(ping_rsps);
                    }
                    else if(ping_rsps.size() >= num_expected_rsps) {
                        return new LinkedList<PingData>(ping_rsps);
                    }

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

        private static int getNumServerResponses(List<PingData> rsps) {
            int cnt=0;
            for(PingData rsp: rsps) {
                if(rsp.isServer())
                    cnt++;
            }
            return cnt;
        }

        private static boolean containsCoordinatorResponse(List<PingData> rsps) {
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