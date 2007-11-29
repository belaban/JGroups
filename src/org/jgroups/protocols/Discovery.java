
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.TimeScheduler;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * The Discovery protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by specific subclasses, e.g. by mcasting PING
 * requests to an IP MCAST address or, if gossiping is enabled, by contacting the GossipRouter.
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * <ul>
 * <li>timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
 * <li>num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
 * <li>num_ping_requests - the number of GET_MBRS_REQ messages to be sent (min=1), distributed over timeout ms
 * </ul>
 * @author Bela Ban
 * @version $Id: Discovery.java,v 1.37 2007/11/29 11:27:36 belaban Exp $
 */
public abstract class Discovery extends Protocol {
    final Vector<Address>	members=new Vector<Address>(11);
    Address					local_addr=null;
    String					group_addr=null;
    long					timeout=3000;
    int						num_initial_members=2;
    boolean					is_server=false;
    TimeScheduler			timer=null;

    /** Number of GET_MBRS_REQ messages to be sent (min=1), distributed over timeout ms */
    int                     num_ping_requests=2;
    int                     num_discovery_requests=0;


    private final Set<Responses> ping_responses=new HashSet<Responses>();
    private final PingSenderTask sender=new PingSenderTask(timeout, num_ping_requests);
    

    
    public void init() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("timer cannot be retrieved from protocol stack");
    }


    /** Called after local_addr was set */
    public void localAddressSet(Address addr) {
    }

    public abstract void sendGetMembersRequest(String cluster_name);


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

    /**
     * sets the properties of the PING protocol.
     * The following properties are available
     * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
     * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
     * @param props - a property set
     * @return returns true if all properties were parsed properly
     *         returns false if there are unrecnogized properties in the property set
     */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");              // max time to wait for initial members
        if(str != null) {
            timeout=Long.parseLong(str);
            if(timeout <= 0) {
                if(log.isErrorEnabled()) log.error("timeout must be > 0");
                return false;
            }
            props.remove("timeout");
        }

        str=props.getProperty("num_initial_members");  // wait for at most n members
        if(str != null) {
            num_initial_members=Integer.parseInt(str);
            props.remove("num_initial_members");
        }

        str=props.getProperty("num_ping_requests");  // number of GET_MBRS_REQ messages
        if(str != null) {
            num_ping_requests=Integer.parseInt(str);
            props.remove("num_ping_requests");
            if(num_ping_requests < 1)
                num_ping_requests=1;
        }

        if(!props.isEmpty()) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            if(log.isErrorEnabled()) log.error("The following properties are not recognized: " + sb);
            return false;
        }
        return true;
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
     * @return List<PingRsp>
     */
    public List<PingRsp> findInitialMembers(Promise promise) {
        num_discovery_requests++;

        final Responses rsps=new Responses(num_initial_members, promise);
        synchronized(ping_responses) {
            ping_responses.add(rsps);
        }

        sender.start(group_addr);
        try {
            return rsps.get(timeout);
        }
        catch(Exception e) {
            return null;
        }
        finally {
        	sender.stop();
            synchronized(ping_responses) {
                ping_responses.remove(rsps);
            }
        }
    }


    public String findInitialMembersAsString() {
    	List<PingRsp> results=findInitialMembers(null);
        if(results == null || results.isEmpty()) return "<empty>";
        StringBuilder sb=new StringBuilder();
        for(PingRsp rsp: results) {
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
        Message msg, rsp_msg;
        PingHeader rsp_hdr;
        PingRsp rsp;
        Address coord;

        switch(evt.getType()) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            PingHeader hdr=(PingHeader)msg.getHeader(getName());
            if(hdr == null) {
                return up_prot.up(evt);
            }

            switch(hdr.type) {

            case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
                if(local_addr != null && msg.getSrc() != null && local_addr.equals(msg.getSrc())) {
                    return null;
                }

                if(group_addr == null || hdr.cluster_name == null) {
                    if(log.isWarnEnabled())
                        log.warn("group_addr (" + group_addr + ") or cluster_name of header (" + hdr.cluster_name
                        + ") is null; passing up discovery request from " + msg.getSrc() + ", but this should not" +
                                "be the case");
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

                synchronized(members) {
                    coord=!members.isEmpty()? members.firstElement() : local_addr;
                }

                PingRsp ping_rsp=new PingRsp(local_addr, coord, is_server);
                rsp_msg=new Message(msg.getSrc(), null, null);
                rsp_msg.setFlag(Message.OOB);
                rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
                rsp_msg.putHeader(getName(), rsp_hdr);
                if(log.isTraceEnabled())
                    log.trace("received GET_MBRS_REQ from " + msg.getSrc() + ", sending response " + rsp_hdr);
                down_prot.down(new Event(Event.MSG, rsp_msg));
                return null;

            case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                rsp=hdr.arg;

                if(log.isTraceEnabled())
                    log.trace("received GET_MBRS_RSP, rsp=" + rsp);
                // myfuture.addResponse(rsp);

                synchronized(ping_responses) {
                    for(Responses rsps: ping_responses)
                        rsps.addResponse(rsp);
                }
                return null;

            default:
                if(log.isWarnEnabled()) log.warn("got PING header with unknown type (" + hdr.type + ')');
                return null;
            }

        case Event.SET_LOCAL_ADDRESS:
            up_prot.up(evt);
            local_addr=(Address)evt.getArg();
            localAddressSet(local_addr);
            break;

        default:
            up_prot.up(evt);            // Pass up to the layer above us
            break;
        }

        return null;
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
            return findInitialMembers((Promise)evt.getArg());

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

        case Event.CONNECT:
        case Event.CONNECT_WITH_STATE_TRANSFER:    
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

    

    class PingSenderTask {
        private double		interval;
		private Future		senderFuture;

        public PingSenderTask(long timeout, int num_requests) {
            interval=timeout / (double)num_requests;
        }

        public synchronized void start(final String cluster_name) {
            if(senderFuture == null || senderFuture.isDone()) {
                senderFuture=timer.scheduleWithFixedDelay(new Runnable() {
                    public void run() {
                        try {
                            sendGetMembersRequest(cluster_name);
                        }
                        catch(Exception ex) {
                            if(log.isErrorEnabled())
                                log.error("failed sending discovery request", ex);
                        }
                    }
                }, 0, (long)interval, TimeUnit.MILLISECONDS);
            }
        }

        public synchronized void stop() {
            if(senderFuture != null) {
                senderFuture.cancel(true);
                senderFuture=null;
            }
        }
    }


    private static class Responses {
        final Promise       promise;
        final List<PingRsp> ping_rsps=new LinkedList<PingRsp>();
        final int           num_expected_rsps;

        public Responses(int num_expected_rsps, Promise promise) {
            this.num_expected_rsps=num_expected_rsps;
            this.promise=promise != null? promise : new Promise();
        }

        public void addResponse(PingRsp rsp) {
            if(rsp == null)
                return;
            promise.getLock().lock();
            try {
                if(!ping_rsps.contains(rsp)) {
                    ping_rsps.add(rsp);
                    if(ping_rsps.size() >= num_expected_rsps)
                        promise.getCond().signalAll();
                }
            }
            finally {
                promise.getLock().unlock();
            }
        }

        public List<PingRsp> get(long timeout) throws InterruptedException, ExecutionException, TimeoutException {
            long start_time=System.currentTimeMillis(), time_to_wait=timeout;

            promise.getLock().lock();
            try {
                while(ping_rsps.size() < num_expected_rsps && time_to_wait > 0 && !promise.hasResult()) {
                    promise.getCond().await(time_to_wait, TimeUnit.MILLISECONDS);
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }
                return new LinkedList<PingRsp>(ping_rsps);
            }
            finally {
                promise.getLock().unlock();
            }
        }

    }
    

//    private static class MyFuture implements Future<List<PingRsp>> {
//        final int num_expected_rsps;
//        @GuardedBy("lock")
//        final List<PingRsp> responses=new LinkedList<PingRsp>();
//        @GuardedBy("lock")
//        volatile boolean cancelled=false;
//        @GuardedBy("lock")
//        volatile boolean done=false;
//        final Lock lock;
//        Condition all_responses_received;
//
//
//        public MyFuture(int num_expected_rsps) {
//            this.num_expected_rsps=num_expected_rsps;
//            this.lock=new ReentrantLock();
//            all_responses_received=lock.newCondition();
//        }
//
//        public MyFuture(int num_expected_rsps, Lock lock, Condition cond) {
//            this.num_expected_rsps=num_expected_rsps;
//            this.lock=lock;
//            all_responses_received=cond;
//        }
//
//        public boolean cancel(boolean b) {
//            lock.lock();
//            try {
//                boolean retval=!cancelled;
//                cancelled=true;
//                responses.clear();
//                all_responses_received.signalAll();
//                return retval;
//            }
//            finally {
//                lock.unlock();
//            }
//        }
//
//        public boolean isCancelled() {
//            return cancelled;
//        }
//
//        public boolean isDone() {
//            return done;
//        }
//
//        public List<PingRsp> get() throws InterruptedException, ExecutionException {
//            lock.lock();
//            try {
//                while(responses.size() < num_expected_rsps && !cancelled) {
//                    all_responses_received.await();
//                }
//                return _get();
//            }
//            finally {
//                lock.unlock();
//            }
//        }
//
//        public List<PingRsp> get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
//            long start_time=System.currentTimeMillis(), time_to_wait=timeout;
//
//            lock.lock();
//            try {
//                while(responses.size() < num_expected_rsps && time_to_wait > 0 && !cancelled) {
//                    all_responses_received.await(time_to_wait, TimeUnit.MILLISECONDS);
//                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
//                }
//                if(responses.size() >= num_expected_rsps || time_to_wait <= 0)
//                    done=true;
//                return _get();
//            }
//            finally {
//                lock.unlock();
//            }
//        }
//
//
//        void addResponse(PingRsp rsp) {
//            if(rsp == null)
//                return;
//            lock.lock();
//            try {
//                if(!responses.contains(rsp)) {
//                    responses.add(rsp);
//                    if(responses.size() >= num_expected_rsps)
//                        done=true;
//                    all_responses_received.signalAll();
//                }
//            }
//            finally {
//                lock.unlock();
//            }
//        }
//
//        void reset() {
//            lock.lock();
//            try {
//                cancelled=false;
//                done=false;
//                responses.clear();
//                all_responses_received.signalAll();
//            }
//            finally {
//                lock.unlock();
//            }
//        }
//
//        private List<PingRsp> _get() {
//            return cancelled? null : new LinkedList<PingRsp>(responses);
//        }
//    }


}
