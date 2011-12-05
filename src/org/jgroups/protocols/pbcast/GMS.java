package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GmsImpl.Request;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.Queue;
import org.jgroups.util.UUID;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Group membership protocol. Handles joins/leaves/crashes (suspicions) and
 * emits new views accordingly. Use VIEW_ENFORCER on top of this layer to make
 * sure new members don't receive any messages until they are members
 * 
 * @author Bela Ban
 */
@MBean(description="Group membership protocol")
public class GMS extends Protocol implements DiagnosticsHandler.ProbeHandler {
    private static final String CLIENT="Client";
    private static final String COORD="Coordinator";
    private static final String PART="Participant";

    /* ------------------------------------------ Properties  ------------------------------------------ */

    @Property(description="Join timeout")
    long join_timeout=5000;

    @Property(description="Leave timeout")
    long leave_timeout=5000;
    
    @Property(description="Timeout (in ms) to complete merge")
    long merge_timeout=5000; // time to wait for all MERGE_RSPS

    @Property(description="Print local address of this member after connect. Default is true")
    private boolean print_local_addr=true;

    @Property(description="Print physical address(es) on startup")
    private boolean print_physical_addrs=true;
    
    /**
     * Setting this to false disables concurrent startups. This is only used by
     * unit testing code for testing merging. To everybody else: don't change it
     * to false !
     */
    @Property(description="Temporary switch. Default is true and should not be changed")
    boolean handle_concurrent_startup=true;
    
    /**
     * Whether view bundling (http://jira.jboss.com/jira/browse/JGRP-144) should
     * be enabled or not. Setting this to false forces each JOIN/LEAVE/SUPSECT
     * request to be handled separately. By default these requests are processed
     * together if they are queued at approximately the same time
     */
    @Property(description="View bundling toggle")
    private boolean view_bundling=true;
    
    @Property(description="Max view bundling timeout if view bundling is turned on. Default is 50 msec")
    private long max_bundling_time=50; // 50ms max to wait for other JOIN, LEAVE or SUSPECT requests
    
    @Property(description="Max number of old members to keep in history. Default is 50")
    protected int num_prev_mbrs=50;

     @Property(description="Number of views to store in history")
    int num_prev_views=20;
    
    @Property(description="Time in ms to wait for all VIEW acks (0 == wait forever. Default is 2000 msec" )
    long view_ack_collection_timeout=2000;
 
    @Property(description="Timeout to resume ViewHandler")
    long resume_task_timeout=20000;

    @Property(description="Use flush for view changes. Default is true")
    boolean use_flush_if_present=true;
    
    @Property(description="Logs failures for collecting all view acks if true")
    boolean log_collect_msgs=true;

    @Property(description="Logs warnings for reception of views less than the current, and for views which don't include self")
    boolean log_view_warnings=true;


    /* --------------------------------------------- JMX  ---------------------------------------------- */
    
    
    private int num_views=0;

    /** Stores the last 20 views */
    private BoundedList<Tuple<View,Long>> prev_views;


    /* --------------------------------------------- Fields ------------------------------------------------ */

    @Property(converter=PropertyConverters.FlushInvoker.class,name="flush_invoker_class")
    protected Class<Callable<Boolean>> flushInvokerClass;
    
    private GmsImpl impl=null;
    private final Object impl_mutex=new Object(); // synchronizes event entry into impl
    private final Map<String,GmsImpl> impls=new HashMap<String,GmsImpl>(3);

    // Handles merge related tasks
    final Merger merger=new Merger(this);

    protected Address local_addr=null;
    protected final Membership members=new Membership(); // real membership
    
    private final Membership tmp_members=new Membership(); // base for computing next view

    /** Members joined but for which no view has been received yet */
    private final List<Address> joining=new ArrayList<Address>(7);

    /** Members excluded from group, but for which no view has been received yet */
    private final List<Address> leaving=new ArrayList<Address>(7);

    /** Keeps track of old members (up to num_prev_mbrs) */
    private BoundedList<Address> prev_members=null;

    protected View view=null;
    
    protected long ltime=0;

    protected TimeScheduler timer=null;    

    /** Class to process JOIN, LEAVE and MERGE requests */
    private final ViewHandler view_handler=new ViewHandler();

    /** To collect VIEW_ACKs from all members */
    protected final AckCollector ack_collector=new AckCollector();

    //[JGRP-700] - FLUSH: flushing should span merge
    protected final AckCollector merge_ack_collector=new AckCollector();

    boolean flushProtocolInStack=false;



    public GMS() {
        initState();
    }

    public ViewId getViewId() {return view != null? view.getViewId() : null;}

    @ManagedAttribute
    public String getView() {return view != null? view.getViewId().toString() : "null";}
    @ManagedAttribute
    public int getNumberOfViews() {return num_views;}
    @ManagedAttribute
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute
    public String getMembers() {return members.toString();}
    @ManagedAttribute
    public int getNumMembers() {return members.size();}
    public long getJoinTimeout() {return join_timeout;}
    public void setJoinTimeout(long t) {join_timeout=t;}
    public long getMergeTimeout() {return merge_timeout;}
    public void setMergeTimeout(long timeout) {merge_timeout=timeout;}

    @ManagedAttribute(description="Stringified version of merge_id")
    public String getMergeId() {return merger.getMergeIdAsString();}

    @ManagedAttribute(description="Is a merge currently running")
    public boolean isMergeInProgress() {return merger.isMergeInProgress();}

    /** Only used for internal testing, don't use this method ! */
    public Merger getMerger() {return merger;}

    @ManagedOperation(description="Prints the last (max 20) MergeIds")
    public String printMergeIdHistory() {return merger.getMergeIdHistory();}

    @ManagedOperation
    public String printPreviousMembers() {
        StringBuilder sb=new StringBuilder();
        if(prev_members != null) {
            for(Address addr: prev_members) {
                sb.append(addr).append("\n");
            }
        }
        return sb.toString();
    }

    public void setPrintLocalAddress(boolean flag) {print_local_addr=flag;}
    public void setPrintLocalAddr(boolean flag) {setPrintLocalAddress(flag);}

    public long getViewAckCollectionTimeout() {
        return view_ack_collection_timeout;
    }

    public void setViewAckCollectionTimeout(long view_ack_collection_timeout) {
        if(view_ack_collection_timeout <= 0)
            throw new IllegalArgumentException("view_ack_collection_timeout has to be greater than 0");
        this.view_ack_collection_timeout=view_ack_collection_timeout;
    }

    public boolean isViewBundling() {
        return view_bundling;
    }

    public void setViewBundling(boolean view_bundling) {
        this.view_bundling=view_bundling;
    }

    public long getMaxBundlingTime() {
        return max_bundling_time;
    }

    public void setMaxBundlingTime(long max_bundling_time) {
        this.max_bundling_time=max_bundling_time;
    }

    @ManagedAttribute
    public int getViewHandlerSize() {return view_handler.size();}
    @ManagedAttribute
    public boolean isViewHandlerSuspended() {return view_handler.suspended();}
    @ManagedOperation
    public String dumpViewHandlerQueue() {
        return view_handler.dumpQueue();
    }
    @ManagedOperation
    public String dumpViewHandlerHistory() {
        return view_handler.dumpHistory();
    }
    @ManagedOperation
    public void suspendViewHandler() {
        view_handler.suspend();
    }
    @ManagedOperation
    public void resumeViewHandler() {
        view_handler.resumeForce();
    }

    Log getLog() {return log;}

    ViewHandler getViewHandler() {return view_handler;}

    @ManagedOperation
    public String printPreviousViews() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<View,Long> tmp: prev_views)
            sb.append(new Date(tmp.getVal2())).append(": ").append(tmp.getVal1()).append("\n");
        return sb.toString();
    }

    @ManagedOperation
    public void suspect(String suspected_member) {
        if(suspected_member == null)
            return;
        Map<Address,String> contents=UUID.getContents();
        for(Map.Entry<Address,String> entry: contents.entrySet()) {
            String logical_name=entry.getValue();
            if(logical_name != null && logical_name.equals(suspected_member)) {
                Address suspect=entry.getKey();
                if(suspect != null)
                    up(new Event(Event.SUSPECT, suspect));
            }
        }
    }

    public boolean isCoordinator() {
        Address coord=determineCoordinator();
        return coord != null && local_addr != null && local_addr.equals(coord);
    }

    public MergeId _getMergeId() {
        return impl instanceof CoordGmsImpl? ((CoordGmsImpl)impl).getMergeId() : null;
    }

    public void setLogCollectMessages(boolean flag) {
        log_collect_msgs=flag;
    }

    public boolean getLogCollectMessages() {
        return log_collect_msgs;
    }

    public void resetStats() {
        super.resetStats();
        num_views=0;
        prev_views.clear();
    }


    public List<Integer> requiredDownServices() {
        return Arrays.asList(Event.GET_DIGEST, Event.SET_DIGEST, Event.FIND_INITIAL_MBRS, Event.FIND_ALL_VIEWS);
    }

    public List<Integer> providedDownServices() {
        return Arrays.asList(Event.IS_MERGE_IN_PROGRESS);
    }
    
    public void setImpl(GmsImpl new_impl) {
        synchronized(impl_mutex) {
            if(impl == new_impl) // unnecessary ?
                return;
            impl=new_impl;
        }
    }


    public GmsImpl getImpl() {
        return impl;
    }


    public void init() throws Exception {
        if(view_ack_collection_timeout <= 0)
            throw new IllegalArgumentException("view_ack_collection_timeout has to be greater than 0");
        if(merge_timeout <= 0)
            throw new IllegalArgumentException("merge_timeout has to be greater than 0");
        prev_members=new BoundedList<Address>(num_prev_mbrs);
        prev_views=new BoundedList<Tuple<View,Long>>(num_prev_views);
        TP transport=getTransport();
        timer=transport.getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(impl != null)
            impl.init();
        transport.registerProbeHandler(this);
    }

    public void start() throws Exception {
        if(impl != null) impl.start();
    }

    public void stop() {
        view_handler.stop(true);
        if(impl != null) impl.stop();
        if(prev_members != null)
            prev_members.clear();
    }


    public void becomeCoordinator() {
        CoordGmsImpl tmp=(CoordGmsImpl)impls.get(COORD);
        if(tmp == null) {
            tmp=new CoordGmsImpl(this);
            impls.put(COORD, tmp);
        }
        try {
            tmp.init();
        }
        catch(Exception e) {
            log.error("exception switching to coordinator role", e);
        }
        setImpl(tmp);
    }


    public void becomeParticipant() {
        ParticipantGmsImpl tmp=(ParticipantGmsImpl)impls.get(PART);

        if(tmp == null) {
            tmp=new ParticipantGmsImpl(this);
            impls.put(PART, tmp);
        }
        try {
            tmp.init();
        }
        catch(Exception e) {
            log.error("exception switching to participant", e);
        }
        setImpl(tmp);
    }

    public void becomeClient() {
        ClientGmsImpl tmp=(ClientGmsImpl)impls.get(CLIENT);
        if(tmp == null) {
            tmp=new ClientGmsImpl(this);
            impls.put(CLIENT, tmp);
        }
        try {
            tmp.init();
        }
        catch(Exception e) {
            log.error("exception switching to client role", e);
        }
        setImpl(tmp);
    }


    boolean haveCoordinatorRole() {
        return impl != null && impl instanceof CoordGmsImpl;
    }

    @ManagedOperation(description="Fetches digests from all members and installs them, unblocking blocked members")
    public void fixDigests() {
        if(impl instanceof CoordGmsImpl)
            ((CoordGmsImpl)impl).fixDigests();
    }

    @ManagedOperation(description="Forces cancellation of current merge task")
    public void cancelMerge() {
        merger.forceCancelMerge();
    }

    @ManagedAttribute(description="Is the merge task running")
    public boolean isMergeTaskRunning() {return merger.isMergeTaskRunning();}

    @ManagedAttribute(description="Is the merge killer task running")
    public boolean isMergeKillerRunning() {return merger.isMergeKillerTaskRunning();}

    /**
     * Computes the next view. Returns a copy that has <code>old_mbrs</code> and
     * <code>suspected_mbrs</code> removed and <code>new_mbrs</code> added.
     */
    public View getNextView(Collection<Address> new_mbrs, Collection<Address> old_mbrs, Collection<Address> suspected_mbrs) {
        synchronized(members) {
            ViewId view_id=view != null? view.getViewId() : null;
            if(view_id == null) {
                log.error("view_id is null");
                return null; // this should *never* happen !
            }
            long vid=Math.max(view_id.getId(), ltime) + 1;
            ltime=vid;
            Membership tmp_mbrs=tmp_members.copy();  // always operate on the temporary membership
            tmp_mbrs.remove(suspected_mbrs);
            tmp_mbrs.remove(old_mbrs);
            tmp_mbrs.add(new_mbrs);
            List<Address> mbrs=tmp_mbrs.getMembers();
            Address new_coord=local_addr;
            if(!mbrs.isEmpty())
                new_coord=mbrs.get(0);
            View v=new View(new_coord, vid, mbrs);

            // Update membership (see DESIGN for explanation):
            tmp_members.set(mbrs);

            // Update joining list (see DESIGN for explanation)
            if(new_mbrs != null) {
                for(Address tmp_mbr: new_mbrs) {
                    if(!joining.contains(tmp_mbr))
                        joining.add(tmp_mbr);
                }
            }

            // Update leaving list (see DESIGN for explanations)
            if(old_mbrs != null) {
                for(Address addr: old_mbrs) {
                    if(!leaving.contains(addr))
                        leaving.add(addr);
                }
            }
            if(suspected_mbrs != null) {
                for(Address addr:suspected_mbrs) {
                    if(!leaving.contains(addr))
                        leaving.add(addr);
                }
            }
            return v;
        }
    }


    /**
     * Broadcasts the new view and digest, and waits for acks from all members in the list given as argument.
     * If the list is null, we take the members who are part of new_view
     */
    public void castViewChange(View new_view, Digest digest, JoinRsp jr, Collection<Address> newMembers) {
        if(log.isTraceEnabled())
            log.trace(local_addr + ": mcasting view " + new_view + " (" + new_view.size() + " mbrs)\n");

        // Send down a local TMP_VIEW event. This is needed by certain layers (e.g. NAKACK) to compute correct digest
        // in case client's next request (e.g. getState()) reaches us *before* our own view change multicast.
        // Check NAKACK's TMP_VIEW handling for details   
        down_prot.up(new Event(Event.TMP_VIEW, new_view));
        down_prot.down(new Event(Event.TMP_VIEW, new_view));

        List<Address> ackMembers=new ArrayList<Address>(new_view.getMembers());
        if(newMembers != null && !newMembers.isEmpty())
            ackMembers.removeAll(newMembers);

        Message view_change_msg=new Message(); // bcast to all members
        GmsHeader hdr=new GmsHeader(GmsHeader.VIEW, new_view);
        hdr.my_digest=digest;
        view_change_msg.putHeader(this.id, hdr);

         // If we're the only member the VIEW is broadcast to, let's simply install the view directly, without
         // sending the VIEW multicast ! Or else N-1 members drop the multicast anyway...
        if(local_addr != null && ackMembers.size() == 1 && ackMembers.get(0).equals(local_addr)) {
            // we need to add the message to the retransmit window (e.g. in NAKACK), so (1) it can be retransmitted and
            // (2) we increment the seqno (otherwise, we'd return an incorrect digest)
            down_prot.down(new Event(Event.ADD_TO_XMIT_TABLE, view_change_msg));
            impl.handleViewChange(new_view, digest);
        }
        else {
            if(!ackMembers.isEmpty())
                ack_collector.reset(ackMembers);

            down_prot.down(new Event(Event.MSG, view_change_msg));
            try {
                if(!ackMembers.isEmpty()) {
                    ack_collector.waitForAllAcks(view_ack_collection_timeout);
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": received all " + ack_collector.expectedAcks() +
                                    " ACKs from members for view " + new_view.getVid());
                }
            }
            catch(TimeoutException e) {
                if(log_collect_msgs && log.isWarnEnabled()) {
                    log.warn(local_addr + ": failed to collect all ACKs (expected=" + ack_collector.expectedAcks()
                               + ") for view " + new_view.getViewId() + " after " + view_ack_collection_timeout +
                               "ms, missing ACKs from " + ack_collector.printMissing());
                }
            }
        }

        if(jr != null && (newMembers != null && !newMembers.isEmpty())) {
            ack_collector.reset(new ArrayList<Address>(newMembers));
            for(Address joiner: newMembers) {
                sendJoinResponse(jr, joiner);
            }
            try {
                ack_collector.waitForAllAcks(view_ack_collection_timeout);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": received all ACKs (" + ack_collector.expectedAcks() +
                            ") from joiners for view " + new_view.getVid());
            }
            catch(TimeoutException e) {
                if(log_collect_msgs && log.isWarnEnabled()) {
                    log.warn(local_addr + ": failed to collect all ACKs (expected=" + ack_collector.expectedAcks()
                            + ") for unicast view " + new_view + " after " + view_ack_collection_timeout + "ms, missing ACKs from "
                            + ack_collector.printMissing());
                }
            }
        }
    }

    public void sendJoinResponse(JoinRsp rsp, Address dest) {
        Message m=new Message(dest, null, null);        
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, rsp);
        m.putHeader(this.id, hdr);
        getDownProtocol().down(new Event(Event.MSG, m));        
    }


    public void installView(View new_view) {
        installView(new_view, null);
    }


    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack. If the view is a MergeView (subclass
     * of View), then digest will be non-null and has to be set before installing the view.
     */
    public void installView(View new_view, Digest digest) {
        ViewId vid=new_view.getVid();
        List<Address> mbrs=new_view.getMembers();
        ltime=Math.max(vid.getId(), ltime);  // compute the logical time, regardless of whether the view is accepted

        // Discards view with id lower than or equal to our own. Will be installed without check if it is the first view
        if(view != null) {
            ViewId view_id=view.getViewId();
            int rc=vid.compareToIDs(view_id);
            if(rc <= 0) {
                if(log.isWarnEnabled() && rc < 0 && log_view_warnings) { // only scream if view is smaller, silently discard same views
                    log.warn(local_addr + ": received view < current view;" +
                               " discarding it (current vid: " + view_id + ", new vid: " + vid + ')');
                }
                return;
            }
        }

        /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
           This ensures that messages sent in view V1 are only received by members of V1 */
        if(!mbrs.contains(local_addr)) {
            if(log.isWarnEnabled() && log_view_warnings)
                log.warn(local_addr + ": not member of view " + new_view.getViewId() + "; discarding it");
            return;
        }

        if(digest != null) {
            if(new_view instanceof MergeView)
                mergeDigest(digest);
            else
                setDigest(digest);
        }

        if(log.isDebugEnabled()) log.debug(local_addr + ": installing view " + new_view);

        Event view_event;
        synchronized(members) {
            view=new View(new_view.getVid(), new_view.getMembers());
            view_event=new Event(Event.VIEW_CHANGE, new_view);

            // Set the membership. Take into account joining members
            if(!mbrs.isEmpty()) {
                members.set(mbrs);
                tmp_members.set(members);
                joining.removeAll(mbrs);  // remove all members in mbrs from joining
                // remove all elements from 'leaving' that are not in 'mbrs'
                leaving.retainAll(mbrs);

                tmp_members.add(joining);    // add members that haven't yet shown up in the membership
                tmp_members.remove(leaving); // remove members that haven't yet been removed from the membership

                // add to prev_members
                for(Address addr: mbrs) {
                    if(!prev_members.contains(addr))
                        prev_members.add(addr);
                }
            }

            Address coord=determineCoordinator();
            if(coord != null && coord.equals(local_addr) && !haveCoordinatorRole()) {
                becomeCoordinator();
            }
            else {
                if(haveCoordinatorRole() && !local_addr.equals(coord)) {
                    becomeParticipant();
                    merge_ack_collector.reset(null); // we don't need this one anymore
                }
            }
        }

        // - Changed order of passing view up and down (http://jira.jboss.com/jira/browse/JGRP-347)
        // - Changed it back (bela Sept 4 2007): http://jira.jboss.com/jira/browse/JGRP-564
        // - Moved sending up view_event out of the synchronized block (bela Nov 2011)
        down_prot.down(view_event); // needed e.g. by failure detector or UDP
        up_prot.up(view_event);

        List<Address> tmp_mbrs=new_view.getMembers();
        ack_collector.retainAll(tmp_mbrs);
        merge_ack_collector.retainAll(tmp_mbrs);

        if(new_view instanceof MergeView)
            merger.forceCancelMerge();

        if(stats) {
            num_views++;
            prev_views.add(new Tuple<View,Long>(new_view, System.currentTimeMillis()));
        }
    }


    protected Address determineCoordinator() {
        synchronized(members) {
            return members.size() > 0? members.elementAt(0) : null;
        }
    }


    /** Checks whether the potential_new_coord would be the new coordinator (2nd in line) */
    protected boolean wouldBeNewCoordinator(Address potential_new_coord) {
        if(potential_new_coord == null) return false;
        synchronized(members) {
            if(members.size() < 2) return false;
            Address new_coord=members.elementAt(1);  // member at 2nd place
            return new_coord != null && new_coord.equals(potential_new_coord);
        }
    }


    /** Send down a SET_DIGEST event */
    public void setDigest(Digest d) {
        down_prot.down(new Event(Event.SET_DIGEST, d));
    }


    /** Send down a MERGE_DIGEST event */
    public void mergeDigest(Digest d) {
        down_prot.down(new Event(Event.MERGE_DIGEST, d));
    }


    /** Sends down a GET_DIGEST event and waits for the GET_DIGEST_OK response, or
     timeout, whichever occurs first */
    public Digest getDigest() {
        return (Digest)down_prot.down(Event.GET_DIGEST_EVT);
    }

    boolean startFlush(View view) {
        return _startFlush(view, 4, 1000L, 5000L);
    }

    boolean startFlush(View view, int maxAttempts, long floor, long ceiling) {
        return _startFlush(view, maxAttempts, floor, ceiling);
    }

    protected boolean _startFlush(final View new_view, int maxAttempts, long randomFloor, long randomCeiling) {
        if(!flushProtocolInStack)
            return true;
        if(flushInvokerClass != null) {
            try {
                Callable<Boolean> invoker = flushInvokerClass.getDeclaredConstructor(View.class).newInstance(new_view);
                return invoker.call();
            } catch (Throwable e) {
                return false;
            }
        }

        try {
            boolean successfulFlush=false;
            boolean validView=new_view != null && new_view.size() > 0;
            if(validView && flushProtocolInStack) {
                int attemptCount = 0;
                while (attemptCount < maxAttempts) {
                    try {
                        up_prot.up(new Event(Event.SUSPEND, new ArrayList<Address>(new_view.getMembers())));
                        successfulFlush = true;
                        break;
                    } catch (Exception e) {
                        Util.sleepRandom(randomFloor, randomCeiling);
                        attemptCount++;
                    }
                }

                if(successfulFlush) {
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": successful GMS flush by coordinator");
                }
                else {
                    if(log.isWarnEnabled())
                        log.warn(local_addr + ": GMS flush by coordinator failed");
                }
            }
            return successfulFlush;
        } catch (Exception e) {
            return false;
        }
    }

    void stopFlush() {
        if(flushProtocolInStack) {
            if(log.isTraceEnabled()) {
                log.trace(local_addr + ": sending RESUME event");
            }
            up_prot.up(new Event(Event.RESUME));
        }
    }
    
    void stopFlush(List<Address> members) {
        if(log.isTraceEnabled()){
            log.trace(local_addr + ": sending RESUME event");
        }
        up_prot.up(new Event(Event.RESUME,members));
    }

    @SuppressWarnings("unchecked")
    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                GmsHeader hdr=(GmsHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                switch(hdr.type) {
                    case GmsHeader.JOIN_REQ:
                        view_handler.add(new Request(Request.JOIN, hdr.mbr, false, null, hdr.useFlushIfPresent));
                        break;
                    case GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                        view_handler.add(new Request(Request.JOIN_WITH_STATE_TRANSFER, hdr.mbr, false, null, hdr.useFlushIfPresent));
                        break;    
                    case GmsHeader.JOIN_RSP:
                        impl.handleJoinResponse(hdr.join_rsp);
                        break;
                    case GmsHeader.LEAVE_REQ:
                        if(hdr.mbr == null)
                            return null;
                        view_handler.add(new Request(Request.LEAVE, hdr.mbr, false));
                        break;
                    case GmsHeader.LEAVE_RSP:
                        impl.handleLeaveResponse();
                        break;
                    case GmsHeader.VIEW:
                        View new_view=hdr.view;
                        if(new_view == null)
                            return null;

                        Address coord=msg.getSrc();
                        if(!new_view.containsMember(coord)) {
                            sendViewAck(coord); // we need to send the ack first, otherwise the connection is removed
                            impl.handleViewChange(new_view, hdr.my_digest);
                        }
                        else {
                            impl.handleViewChange(new_view, hdr.my_digest);
                            sendViewAck(coord); // send VIEW_ACK to sender of view
                        }
                        break;

                    case GmsHeader.VIEW_ACK:
                        Address sender=msg.getSrc();
                        ack_collector.ack(sender);
                        return null; // don't pass further up

                    case GmsHeader.MERGE_REQ:
                        impl.handleMergeRequest(msg.getSrc(), hdr.merge_id, hdr.mbrs);
                        break;

                    case GmsHeader.MERGE_RSP:
                        MergeData merge_data=new MergeData(msg.getSrc(), hdr.view, hdr.my_digest, hdr.merge_rejected);
                        if(log.isTraceEnabled()) {
                            log.trace(local_addr + ": got merge response from " + msg.getSrc() +
                                        ", merge_id=" + hdr.merge_id + ", merge data is "+ merge_data);
                        } 
                        impl.handleMergeResponse(merge_data, hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_MERGE_VIEW:
                        impl.handleMergeView(new MergeData(msg.getSrc(), hdr.view, hdr.my_digest), hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_DIGEST:
                        Digest tmp=hdr.my_digest;
                        down_prot.down(new Event(Event.MERGE_DIGEST, tmp));
                        break;
                     
                    case GmsHeader.INSTALL_MERGE_VIEW_OK:                        
                        //[JGRP-700] - FLUSH: flushing should span merge
                        merge_ack_collector.ack(msg.getSrc());                   
                        break;    

                    case GmsHeader.CANCEL_MERGE:
                        //[JGRP-524] - FLUSH and merge: flush doesn't wrap entire merge process                        
                        impl.handleMergeCancelled(hdr.merge_id);
                        break;

                    case GmsHeader.GET_DIGEST_REQ:
                        // only handle this request if it was sent by the coordinator (or at least a member) of the current cluster
                        synchronized(members) {
                            if(!members.contains(msg.getSrc()))
                                break;
                        }

                        // discard my own request:
                        if(msg.getSrc().equals(local_addr))
                            return null;

                        if(hdr.merge_id !=null && !(merger.matchMergeId(hdr.merge_id) || merger.setMergeId(null, hdr.merge_id)))
                            return null;

                        // fetch only my own digest
                        Digest digest=(Digest)down_prot.down(new Event(Event.GET_DIGEST, local_addr));
                        if(digest != null) {
                            GmsHeader rsp_hdr=new GmsHeader(GmsHeader.GET_DIGEST_RSP);
                            rsp_hdr.my_digest=digest;
                            Message get_digest_rsp=new Message(msg.getSrc(), null, null);
                            get_digest_rsp.setFlag(Message.OOB);
                            get_digest_rsp.putHeader(this.id, rsp_hdr);
                            down_prot.down(new Event(Event.MSG, get_digest_rsp));
                        }
                        break;

                    case GmsHeader.GET_DIGEST_RSP:
                        Digest digest_rsp=hdr.my_digest;
                        impl.handleDigestResponse(msg.getSrc(), digest_rsp);
                        break;

                    default:
                        if(log.isErrorEnabled()) log.error("GmsHeader with type=" + hdr.type + " not known");
                }
                return null;  // don't pass up

            case Event.SUSPECT:
                Object retval=up_prot.up(evt);
                Address suspected=(Address)evt.getArg();
                view_handler.add(new Request(Request.SUSPECT, suspected, true));
                ack_collector.suspect(suspected);
                merge_ack_collector.suspect(suspected);
                return retval;                             

            case Event.UNSUSPECT:
                impl.unsuspect((Address)evt.getArg());
                return null;                              // discard

            case Event.MERGE:
                view_handler.add(new Request(Request.MERGE, null, false, (Map<Address,View>)evt.getArg()));
                return null;                              // don't pass up

            case Event.IS_MERGE_IN_PROGRESS:
                return merger.isMergeInProgress();
        }
        return up_prot.up(evt);
    }



    
    @SuppressWarnings("unchecked")
    public Object down(Event evt) {
        int type=evt.getType();

        switch(type) {
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                boolean use_flush=type == Event.CONNECT_USE_FLUSH || type == Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH;
                boolean state_transfer=type == Event.CONNECT_WITH_STATE_TRANSFER
                        || type == Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH;

                if(print_local_addr) {
                    PhysicalAddress physical_addr=print_physical_addrs?
                            (PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)) : null;
                    System.out.println("\n-------------------------------------------------------------------\n" +
                            "GMS: address=" + local_addr + ", cluster=" + evt.getArg() +
                            (physical_addr != null? ", physical address=" + physical_addr : "") +
                            "\n-------------------------------------------------------------------");
                }
                else {
                    if(log.isDebugEnabled()) {
                        PhysicalAddress physical_addr=print_physical_addrs?
                          (PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)) : null;
                        log.debug("address=" + local_addr + ", cluster=" + evt.getArg() +
                                    (physical_addr != null? ", physical address=" + physical_addr : ""));
                    }
                }
                down_prot.down(evt);
                if(local_addr == null)
                    throw new IllegalStateException("local_addr is null");
                try {
                    if(state_transfer)
                        impl.joinWithStateTransfer(local_addr, use_flush);
                    else
                        impl.join(local_addr, use_flush);
                }
                catch(Throwable e) {
                    return e;
                }
                return null;  // don't pass down: event has already been passed down
                
            case Event.DISCONNECT:
                impl.leave((Address)evt.getArg());
                if(!(impl instanceof CoordGmsImpl)) {
                    initState(); // in case connect() is called again
                }
                down_prot.down(evt); // notify the other protocols, but ignore the result
                return null;

            case Event.CONFIG :
               Map<String,Object> config=(Map<String,Object>)evt.getArg();
               if((config != null && config.containsKey("flush_supported"))){
            	   flushProtocolInStack=true;
               }
               break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return down_prot.down(evt);
    }


    public Map<String, String> handleProbe(String... keys) {
        for(String key: keys) {
            if(key.equals("fix-digests")) {
                fixDigests();
            }
        }
        return null;
    }

    public String[] supportedKeys() {
        return new String[]{"fix-digests"};
    }

    /* ------------------------------- Private Methods --------------------------------- */

    final void initState() {
        becomeClient();
        view=null;
    }


    private void sendViewAck(Address dest) {
        Message view_ack=new Message(dest, null, null);
        view_ack.setFlag(Message.OOB);
        GmsHeader tmphdr=new GmsHeader(GmsHeader.VIEW_ACK);
        view_ack.putHeader(this.id, tmphdr);
        down_prot.down(new Event(Event.MSG,view_ack));
    }

    /* --------------------------- End of Private Methods ------------------------------- */



    public static class GmsHeader extends Header {
        public static final byte JOIN_REQ=1;
        public static final byte JOIN_RSP=2;
        public static final byte LEAVE_REQ=3;
        public static final byte LEAVE_RSP=4;
        public static final byte VIEW=5;
        public static final byte MERGE_REQ=6;
        public static final byte MERGE_RSP=7;
        public static final byte INSTALL_MERGE_VIEW=8;
        public static final byte CANCEL_MERGE=9;
        public static final byte VIEW_ACK=10;
        public static final byte JOIN_REQ_WITH_STATE_TRANSFER = 11;
        public static final byte INSTALL_MERGE_VIEW_OK=12;
        public static final byte GET_DIGEST_REQ=13;
        public static final byte GET_DIGEST_RSP=14;
        public static final byte INSTALL_DIGEST=15;


        byte type=0;
        View view=null;            // used when type=VIEW or MERGE_RSP or INSTALL_MERGE_VIEW
        Address mbr=null;             // used when type=JOIN_REQ or LEAVE_REQ
        Collection<? extends Address> mbrs=null; // used with MERGE_REQ
        boolean useFlushIfPresent; // used when type=JOIN_REQ
        JoinRsp join_rsp=null;        // used when type=JOIN_RSP
        Digest my_digest=null;          // used when type=MERGE_RSP or INSTALL_MERGE_VIEW
        MergeId merge_id=null;        // used when type=MERGE_REQ or MERGE_RSP or INSTALL_MERGE_VIEW or CANCEL_MERGE
        boolean merge_rejected=false; // used when type=MERGE_RSP


        public GmsHeader() {
        } // used for Externalization

        public GmsHeader(byte type) {
            this.type=type;
        }


        /** Used for VIEW header */
        public GmsHeader(byte type, View view) {
            this.type=type;
            this.view=view;
        }


        /** Used for JOIN_REQ or LEAVE_REQ header */
        public GmsHeader(byte type, Address mbr,boolean useFlushIfPresent) {
            this.type=type;
            this.mbr=mbr;
            this.useFlushIfPresent = useFlushIfPresent;
        }
        
        public GmsHeader(byte type, Address mbr) {
        	this(type,mbr,true);
        }

        public GmsHeader(byte type, Collection<Address> mbrs) {
            this(type);
            this.mbrs=mbrs;
        }

        /** Used for JOIN_RSP header */
        public GmsHeader(byte type, JoinRsp join_rsp) {
            this.type=type;
            this.join_rsp=join_rsp;
        }

        public byte getType() {
            return type;
        }

        public Address getMember() {
            return mbr;
        }

        public MergeId getMergeId() {
            return merge_id;
        }

        public void setMergeId(MergeId merge_id) {
            this.merge_id=merge_id;
        }

        public boolean isMergeRejected() {
            return merge_rejected;
        }

        public void setMergeRejected(boolean merge_rejected) {
            this.merge_rejected=merge_rejected;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder("GmsHeader");
            sb.append('[' + type2String(type) + ']');
            switch(type) {
                case JOIN_REQ:
                case LEAVE_REQ:
                case GET_DIGEST_REQ:
                    sb.append(": mbr=" + mbr);
                    break;

                case JOIN_RSP:
                    sb.append(": join_rsp=" + join_rsp);
                    break;

                case VIEW:
                case VIEW_ACK:
                    sb.append(": view=" + view);
                    break;

                case MERGE_REQ:
                    sb.append(": merge_id=" + merge_id).append(", mbrs=" + mbrs);
                    break;

                case MERGE_RSP:
                    sb.append(": view=" + view + ", digest=" + my_digest + ", merge_id=" + merge_id);
                    if(merge_rejected) sb.append(", merge_rejected=" + merge_rejected);
                    break;

                case INSTALL_MERGE_VIEW:
                case GET_DIGEST_RSP:
                case INSTALL_DIGEST:
                    sb.append(": view=" + view + ", digest=" + my_digest);
                    break;

                case CANCEL_MERGE:
                    sb.append(", <merge cancelled>, merge_id=" + merge_id);
                    break;
            }
            return sb.toString();
        }


        public static String type2String(int type) {
            switch(type) {
                case JOIN_REQ: return "JOIN_REQ";
                case JOIN_RSP: return "JOIN_RSP";
                case LEAVE_REQ: return "LEAVE_REQ";
                case LEAVE_RSP: return "LEAVE_RSP";
                case VIEW: return "VIEW";
                case MERGE_REQ: return "MERGE_REQ";
                case MERGE_RSP: return "MERGE_RSP";
                case INSTALL_MERGE_VIEW: return "INSTALL_MERGE_VIEW";
                case CANCEL_MERGE: return "CANCEL_MERGE";
                case VIEW_ACK: return "VIEW_ACK";
                case JOIN_REQ_WITH_STATE_TRANSFER: return "JOIN_REQ_WITH_STATE_TRANSFER";
                case INSTALL_MERGE_VIEW_OK: return "INSTALL_MERGE_VIEW_OK";
                case GET_DIGEST_REQ: return "GET_DIGEST_REQ";
                case GET_DIGEST_RSP: return "GET_DIGEST_RSP";
                case INSTALL_DIGEST: return "INSTALL_DIGEST";
                default: return "<unknown>";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            boolean isMergeView=view != null && view instanceof MergeView;
            out.writeBoolean(isMergeView);
            Util.writeStreamable(view, out);
            Util.writeAddress(mbr, out);
            Util.writeAddresses(mbrs, out);
            Util.writeStreamable(join_rsp, out);
            Util.writeStreamable(my_digest, out);
            Util.writeStreamable(merge_id, out);
            out.writeBoolean(merge_rejected);
            out.writeBoolean(useFlushIfPresent);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            boolean isMergeView=in.readBoolean();
            if(isMergeView)
                view=(View)Util.readStreamable(MergeView.class, in);
            else
                view=(View)Util.readStreamable(View.class, in);
            mbr=Util.readAddress(in);
            mbrs=Util.readAddresses(in, ArrayList.class);
            join_rsp=(JoinRsp)Util.readStreamable(JoinRsp.class, in);
            my_digest=(Digest)Util.readStreamable(Digest.class, in);
            merge_id=(MergeId)Util.readStreamable(MergeId.class, in);
            merge_rejected=in.readBoolean();
            useFlushIfPresent=in.readBoolean();
        }

        public int size() {
            int retval=Global.BYTE_SIZE *2; // type + merge_rejected

            retval+=Global.BYTE_SIZE; // presence view
            retval+=Global.BYTE_SIZE; // MergeView or View
            if(view != null)
                retval+=view.serializedSize();

            retval+=Util.size(mbr);

            retval+=Util.size(mbrs);

            retval+=Global.BYTE_SIZE; // presence of join_rsp
            if(join_rsp != null)
                retval+=join_rsp.serializedSize();

            retval+=Global.BYTE_SIZE; // presence for my_digest
            if(my_digest != null)
                retval+=my_digest.serializedSize();

            retval+=Global.BYTE_SIZE; // presence for merge_id
            if(merge_id != null)
                retval+=merge_id.size();
            
            retval+=Global.BYTE_SIZE; // boolean useFlushIfPresent
            return retval;
        }

    }









    /**
     * Class which processes JOIN, LEAVE and MERGE requests. Requests are queued and processed in FIFO order
     * @author Bela Ban
     */
    class ViewHandler implements Runnable {
        volatile Thread                     thread;
        final Queue                         queue=new Queue(); // Queue<Request>
        volatile boolean                    suspended=false;
        final static long                   INTERVAL=5000;
        private static final long           MAX_COMPLETION_TIME=10000;
        /** Maintains a list of the last 20 requests */
        private final BoundedList<String>   history=new BoundedList<String>(20);

        /** Current Resumer task */
        private Future<?>                   resumer;


        synchronized void add(Request req) {
            if(suspended) {
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": queue is suspended; request " + req + " is discarded");
                return;
            }
            start();
            try {
                queue.add(req);
                history.add(new Date() + ": " + req.toString());
            }
            catch(QueueClosedException e) {
                if(log.isTraceEnabled())
                    log.trace("queue is closed; request " + req + " is discarded");
            }
        }


        void waitUntilCompleted(long timeout) {
            waitUntilCompleted(timeout, false);
        }

        synchronized void waitUntilCompleted(long timeout, boolean resume) {
            if(thread != null) {
                try {
                    thread.join(timeout);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt(); // set interrupt flag again
                }
                thread = null; // Added after Brian noticed that ViewHandler leaks class loaders
            }
            if(resume)
                resumeForce();
        }

        synchronized void start() {
            if(queue.closed())
                queue.reset();
            if(thread == null || !thread.isAlive()) {
                thread=getThreadFactory().newThread(this, "ViewHandler");
                thread.setDaemon(false); // thread cannot terminate if we have tasks left, e.g. when we as coord leave
                thread.start();
            }
        }

        synchronized void stop(boolean flush) {
            queue.close(flush);
            if(resumer != null)
                resumer.cancel(false);
        }

        /**
         * Waits until the current requests in the queue have been processed, then clears the queue and discards new
         * requests from now on
         */
        public synchronized void suspend() {
            if(!suspended) {
                suspended=true;
                queue.clear();
                waitUntilCompleted(MAX_COMPLETION_TIME);
                queue.close(true);
                resumer=timer.schedule(new Runnable() {
                    public void run() {
                        resume();
                    }
                }, resume_task_timeout, TimeUnit.MILLISECONDS);
            }
        }


        public synchronized void resume() {
            if(!suspended)
                return;
            if(resumer != null)
                resumer.cancel(false);
            resumeForce();
        }

        public synchronized void resumeForce() {
            if(queue.closed())
                queue.reset();
            suspended=false;
        }

        public void run() {
            long end_time, wait_time;
            List<Request> requests=new LinkedList<Request>();
            while(Thread.currentThread().equals(thread) && !suspended) {
                try {
                    boolean keepGoing=false;
                    end_time=System.currentTimeMillis() + max_bundling_time;
                    do {
                        Request firstRequest=(Request)queue.remove(INTERVAL); // throws a TimeoutException if it runs into timeout
                        requests.add(firstRequest);
                        if(!view_bundling)
                            break;
                        if(queue.size() > 0) {
                            Request nextReq=(Request)queue.peek();
                            keepGoing=view_bundling && firstRequest.canBeProcessedTogether(nextReq);
                        }
                        else {
                            wait_time=end_time - System.currentTimeMillis();
                            if(wait_time > 0)
                                queue.waitUntilClosed(wait_time); // misnomer: waits until element has been added or q closed
                            keepGoing=queue.size() > 0 && firstRequest.canBeProcessedTogether((Request)queue.peek());
                        }
                    }
                    while(keepGoing && System.currentTimeMillis() < end_time);

                    try {
                        process(requests);
                    }
                    finally {
                        requests.clear();
                    }
                }
                catch(QueueClosedException e) {
                    break;
                }
                catch(TimeoutException e) {
                    break;
                }
                catch(Throwable catchall) {
                    Util.sleep(50);
                }
            }
        }

        public int size() {return queue.size();}
        public boolean suspended() {return suspended;}
        public String dumpQueue() {
            StringBuilder sb=new StringBuilder();
            List v=queue.values();
            for(Iterator it=v.iterator(); it.hasNext();) {
                sb.append(it.next() + "\n");
            }
            return sb.toString();
        }

        public String dumpHistory() {
            StringBuilder sb=new StringBuilder();
            for(String line: history) {
                sb.append(line + "\n");
            }
            return sb.toString();
        }

        private void process(List<Request> requests) {
            if(requests.isEmpty())
                return;
            Request firstReq=requests.get(0);
            switch(firstReq.type) {
                case Request.JOIN:
                case Request.JOIN_WITH_STATE_TRANSFER:
                case Request.LEAVE:
                case Request.SUSPECT:                   
                    impl.handleMembershipChange(requests);
                    break;
                case Request.MERGE:
                    impl.merge(firstReq.views);
                    break;                
                default:
                    log.error("request " + firstReq.type + " is unknown; discarded");
            }
        }

    }


}