package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GmsImpl.Request;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.MembershipChangePolicy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.Queue;
import org.jgroups.util.UUID;

import java.io.DataInput;
import java.io.DataOutput;
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
    protected static final String CLIENT="Client";
    protected static final String COORD="Coordinator";
    protected static final String PART="Participant";

    // flags for marshalling
    public static final short VIEW_PRESENT     = 1 << 0;
    public static final short DIGEST_PRESENT   = 1 << 1;
    public static final short MERGE_VIEW       = 1 << 2; // if a view is present, is it a MergeView ?
    public static final short DELTA_VIEW       = 1 << 3; // if a view is present, is it a DeltaView ?
    public static final short READ_ADDRS       = 1 << 4; // if digest needs to read its own addresses (rather than that of view)

    /* ------------------------------------------ Properties  ------------------------------------------ */

    @Property(description="Join timeout")
    protected long join_timeout=3000;

    @Property(description="Leave timeout")
    protected long leave_timeout=1000;

    @Property(description="Timeout (in ms) to complete merge")
    protected long merge_timeout=5000; // time to wait for all MERGE_RSPS

    @Property(description="Number of join attempts before we give up and become a singleton. 0 means 'never give up'.")
    protected long max_join_attempts=10;

    @Property(description="Print local address of this member after connect. Default is true")
    protected boolean print_local_addr=true;

    @Property(description="Print physical address(es) on startup")
    protected boolean print_physical_addrs=true;

    /**
     * Setting this to false disables concurrent startups. This is only used by unit testing code for testing merging.
     * To everybody else: don't change it to false !
     */
    @Property(description="Temporary switch. Default is true and should not be changed")
    @Deprecated
    protected boolean handle_concurrent_startup=true;

    /**
     * Whether view bundling (http://jira.jboss.com/jira/browse/JGRP-144) should be enabled or not. Setting this to
     * false forces each JOIN/LEAVE/SUPSECT request to be handled separately. By default these requests are processed
     * together if they are queued at approximately the same time
     */
    @Property(description="View bundling toggle")
    protected boolean view_bundling=true;

    @Property(description="If true, then GMS is allowed to send VIEW messages with delta views, otherwise " +
      "it always sends full views. See https://issues.jboss.org/browse/JGRP-1354 for details.")
    protected boolean use_delta_views=true;

    @Property(description="Max view bundling timeout if view bundling is turned on. Default is 50 msec")
    protected long max_bundling_time=50; // 50ms max to wait for other JOIN, LEAVE or SUSPECT requests

    @Property(description="Max number of old members to keep in history. Default is 50")
    protected int num_prev_mbrs=50;

    @Property(description="Number of views to store in history")
    protected int num_prev_views=10;

    @Property(description="Time in ms to wait for all VIEW acks (0 == wait forever. Default is 2000 msec" )
    protected long view_ack_collection_timeout=2000;

    @Property(description="Timeout to resume ViewHandler")
    protected long resume_task_timeout=20000;

    @Property(description="Use flush for view changes. Default is true")
    protected boolean use_flush_if_present=true;

    @Property(description="Logs failures for collecting all view acks if true")
    protected boolean log_collect_msgs=false;

    @Property(description="Logs warnings for reception of views less than the current, and for views which don't include self")
    protected boolean log_view_warnings=true;

    @Property(description="Whether or not to install a new view locally first before broadcasting it " +
      "(only done in coord role). Set to true if a state transfer protocol is detected")
    protected boolean install_view_locally_first=false;

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    protected int num_views;

    /** Stores the last 20 views */
    protected BoundedList<String> prev_views;


    /* --------------------------------------------- Fields ------------------------------------------------ */

    @Property(converter=PropertyConverters.FlushInvoker.class,name="flush_invoker_class")
    protected Class<Callable<Boolean>>  flushInvokerClass;

    protected GmsImpl                   impl;
    protected final Object              impl_mutex=new Object(); // synchronizes event entry into impl
    protected final Map<String,GmsImpl> impls=new HashMap<>(3);

    // Handles merge related tasks
    protected Merger                    merger;

    protected Address                   local_addr;
    protected final Membership          members=new Membership(); // real membership

    protected final Membership          tmp_members=new Membership(); // base for computing next view

    // computes new views and merge views
    protected MembershipChangePolicy    membership_change_policy=new DefaultMembershipPolicy();

    /** Members joined but for which no view has been received yet */
    protected final List<Address>       joining=new ArrayList<>(7);

    /** Members excluded from group, but for which no view has been received yet */
    protected final List<Address>       leaving=new ArrayList<>(7);

    /** Keeps track of old members (up to num_prev_mbrs) */
    protected BoundedList<Address>      prev_members;

    protected volatile View             view;

    protected long                      ltime;

    protected TimeScheduler             timer;

    /** Class to process JOIN, LEAVE and MERGE requests */
    protected final ViewHandler         view_handler=new ViewHandler();

    /** To collect VIEW_ACKs from all members */
    protected final AckCollector        ack_collector=new AckCollector();

    //[JGRP-700] - FLUSH: flushing should span merge
    protected final AckCollector        merge_ack_collector=new AckCollector();

    protected boolean                   flushProtocolInStack=false;

    // Has this coord sent its first view since becoming coord ? Used to send a full- or delta- view */
    protected boolean                   first_view_sent;



    public GMS() {
        initState();
    }

    public ViewId getViewId() {return view != null? view.getViewId() : null;}
    public View   view() {return view;}

    /** Returns the current view and digest. Try to find a matching digest twice (if not found on the first try) */
    public Tuple<View,Digest> getViewAndDigest() {
        MutableDigest digest=new MutableDigest(view.getMembersRaw()).set(getDigest());
        return digest.allSet() || digest.set(getDigest()).allSet()? new Tuple<View,Digest>(view, digest) : null;
    }

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
    public GMS joinTimeout(long timeout) {this.join_timeout=timeout; return this;}
    public long getMergeTimeout() {return merge_timeout;}
    public void setMergeTimeout(long timeout) {merge_timeout=timeout;}
    public long getMaxJoinAttempts() {return max_join_attempts;}
    public void setMaxJoinAttempts(long t) {max_join_attempts=t;}

    @ManagedAttribute(description="impl")
    public String getImplementation() {
        return impl == null? "n/a" : impl.getClass().getSimpleName();
    }

    @ManagedAttribute(description="Whether or not the current instance is the coordinator")
    public boolean isCoord() {
        return impl instanceof CoordGmsImpl;
    }

    public MembershipChangePolicy getMembershipChangePolicy() {
        return membership_change_policy;
    }

    public void setMembershipChangePolicy(MembershipChangePolicy membership_change_policy) {
        if(membership_change_policy != null)
            this.membership_change_policy=membership_change_policy;
    }

    @ManagedAttribute(description="Stringified version of merge_id")
    public String getMergeId() {return merger.getMergeIdAsString();}

    @ManagedAttribute(description="Is a merge currently running")
    public boolean isMergeInProgress() {return merger.isMergeInProgress();}

    /** Only used for internal testing, don't use this method ! */
    public Merger getMerger() {return merger;}

    @Property(description="The fully qualified name of a class implementing MembershipChangePolicy.")
    public void setMembershipChangePolicy(String classname) {
        try {
            membership_change_policy=(MembershipChangePolicy)Util.loadClass(classname, getClass()).newInstance();
        }
        catch(Throwable e) {
            throw new IllegalArgumentException("membership_change_policy could not be created", e);
        }
    }

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

    ViewHandler getViewHandler() {return view_handler;}

    @ManagedOperation
    public String printPreviousViews() {
        StringBuilder sb=new StringBuilder();
        for(String view_rep: prev_views)
            sb.append(view_rep).append("\n");
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
        return Arrays.asList(Event.GET_DIGEST, Event.SET_DIGEST, Event.FIND_INITIAL_MBRS, Event.FIND_MBRS);
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
        merger=new Merger(this);
        if(view_ack_collection_timeout <= 0)
            throw new IllegalArgumentException("view_ack_collection_timeout has to be greater than 0");
        if(merge_timeout <= 0)
            throw new IllegalArgumentException("merge_timeout has to be greater than 0");
        prev_members=new BoundedList<>(num_prev_mbrs);
        prev_views=new BoundedList<>(num_prev_views);
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
        Protocol state_transfer_prot=stack.findProtocol(STATE_TRANSFER.class, StreamingStateTransfer.class);
        if(state_transfer_prot != null)
            install_view_locally_first=true;
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
            first_view_sent=false;
            tmp.init();
        }
        catch(Exception e) {
            log.error(Util.getMessage("ExceptionSwitchingToCoordinatorRole"), e);
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
            log.error(Util.getMessage("ExceptionSwitchingToParticipant"), e);
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
            log.error(Util.getMessage("ExceptionSwitchingToClientRole"), e);
        }
        setImpl(tmp);
    }


    boolean haveCoordinatorRole() {
        return impl instanceof CoordGmsImpl;
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
     * Computes the next view. Returns a copy that has <code>leavers</code> and
     * <code>suspected_mbrs</code> removed and <code>joiners</code> added.
     */
    public View getNextView(Collection<Address> joiners, Collection<Address> leavers, Collection<Address> suspected_mbrs) {
        synchronized(members) {
            ViewId view_id=view != null? view.getViewId() : null;
            if(view_id == null) {
                log.error(Util.getMessage("ViewidIsNull"));
                return null; // this should *never* happen !
            }
            long vid=Math.max(view_id.getId(), ltime) + 1;
            ltime=vid;

            List<Address> mbrs=computeNewMembership(tmp_members.getMembers(), joiners, leavers, suspected_mbrs);
            Address new_coord=!mbrs.isEmpty()? mbrs.get(0) : local_addr;
            View v=new View(new_coord, vid, mbrs);

            // Update membership (see DESIGN for explanation):
            tmp_members.set(mbrs);

            // Update joining list (see DESIGN for explanation)
            if(joiners != null)
                for(Address tmp_mbr: joiners)
                    if(!joining.contains(tmp_mbr))
                        joining.add(tmp_mbr);

            // Update leaving list (see DESIGN for explanations)
            if(leavers != null)
                for(Address addr: leavers)
                    if(!leaving.contains(addr))
                        leaving.add(addr);

            if(suspected_mbrs != null)
                for(Address addr:suspected_mbrs)
                    if(!leaving.contains(addr))
                        leaving.add(addr);
            return v;
        }
    }

    /** Computes the regular membership */
    protected List<Address> computeNewMembership(final List<Address> current_members, final Collection<Address> joiners,
                                                 final Collection<Address> leavers, final Collection<Address> suspects) {
        List<Address> joiners_copy, leavers_copy, suspects_copy;
        joiners_copy=joiners == null? Collections.<Address>emptyList() : new ArrayList<>(joiners);
        leavers_copy=leavers == null? Collections.<Address>emptyList() : new ArrayList<>(leavers);
        suspects_copy=suspects == null? Collections.<Address>emptyList() : new ArrayList<>(suspects);

        try {
            List<Address> retval=membership_change_policy.getNewMembership(current_members,joiners_copy,leavers_copy,suspects_copy);
            if(retval == null)
                throw new IllegalStateException("null membership list");
            return retval;
        }
        catch(Throwable t) {
            log.error(Util.getMessage("MembershipChangePolicy"), membership_change_policy.getClass().getSimpleName(), t);
        }

        try {
            return new DefaultMembershipPolicy().getNewMembership(current_members,joiners_copy,leavers_copy,suspects_copy);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("DefaultMembershipChangePolicyFailed"), t);
            return null;
        }
    }

    /** Computes a merge membership */
    protected List<Address> computeNewMembership(final Collection<Collection<Address>> subviews) {
        try {
            List<Address> retval=membership_change_policy.getNewMembership(subviews);
            if(retval == null)
                throw new IllegalStateException("null membership list");
            return retval;
        }
        catch(Throwable t) {
            log.error(Util.getMessage("MembershipChangePolicy"), membership_change_policy.getClass().getSimpleName(), t);
        }

        try {
            return new DefaultMembershipPolicy().getNewMembership(subviews);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("DefaultMembershipChangePolicyFailed"), t);
            return null;
        }
    }


    /**
     * Broadcasts the new view and digest as a VIEW message and waits for acks from existing members
     */
    public void castViewChange(View new_view, Digest digest, Collection<Address> newMembers) {
        log.trace("%s: mcasting view %s (%d mbrs)\n", local_addr, new_view, new_view.size());

        // Send down a local TMP_VIEW event. This is needed by certain layers (e.g. NAKACK) to compute correct digest
        // in case client's next request (e.g. getState()) reaches us *before* our own view change multicast.
        // Check NAKACK's TMP_VIEW handling for details
        up_prot.up(new Event(Event.TMP_VIEW, new_view));
        down_prot.down(new Event(Event.TMP_VIEW, new_view));

        List<Address> ackMembers=new ArrayList<>(new_view.getMembers());
        if(newMembers != null && !newMembers.isEmpty())
            ackMembers.removeAll(newMembers);

        View full_view=new_view;
        if(use_delta_views && view != null && !(new_view instanceof MergeView)) {
            if(!first_view_sent) // send the first view as coord as *full* view
                first_view_sent=true;
            else
                new_view=createDeltaView(view, new_view);
        }

        // bcast to all members
        Message view_change_msg=new Message().putHeader(this.id, new GmsHeader(GmsHeader.VIEW))
          .setBuffer(marshal(new_view, digest));

        // Bypasses SEQUENCER, prevents having to forward a merge view to a remote coordinator
        // (https://issues.jboss.org/browse/JGRP-1484)
        if(new_view instanceof MergeView)
            view_change_msg.setFlag(Message.Flag.NO_TOTAL_ORDER);

        if(install_view_locally_first)
            ackMembers.remove(local_addr); // remove self, as we'll install the view locally

        if(!ackMembers.isEmpty())
            ack_collector.reset(ackMembers);

        if(install_view_locally_first)
            impl.handleViewChange(full_view, digest); // install the view locally first

        down_prot.down(new Event(Event.MSG, view_change_msg));
        try {
            if(!ackMembers.isEmpty()) {
                ack_collector.waitForAllAcks(view_ack_collection_timeout);
                log.trace("%s: got all ACKs (%d) from members for view %s", local_addr, ack_collector.expectedAcks(), new_view.getViewId());
            }
        }
        catch(TimeoutException e) {
            if(log_collect_msgs)
                log.warn("%s: failed to collect all ACKs (expected=%d) for view %s after %dms, missing %d ACKs from %s",
                         local_addr, ack_collector.expectedAcks(), new_view.getViewId(), view_ack_collection_timeout,
                         ack_collector.size(), ack_collector.printMissing());
        }
    }

    public void sendJoinResponses(JoinRsp jr, Collection<Address> newMembers) {
        if(jr != null && newMembers != null && !newMembers.isEmpty()) {
            final ViewId view_id=jr.getView().getViewId();
            ack_collector.reset(new ArrayList<>(newMembers));
            for(Address joiner: newMembers)
                sendJoinResponse(jr, joiner);
            try {
                ack_collector.waitForAllAcks(view_ack_collection_timeout);
                log.trace("%s: got all ACKs (%d) from joiners for view %s", local_addr, ack_collector.expectedAcks(), view_id);
            }
            catch(TimeoutException e) {
                if(log_collect_msgs)
                    log.warn("%s: failed to collect all ACKs (expected=%d) for unicast view %s after %dms, missing %d ACKs from %s",
                             local_addr, ack_collector.expectedAcks(), view_id, view_ack_collection_timeout,
                             ack_collector.size(), ack_collector.printMissing());
            }
        }
    }

    public void sendJoinResponse(JoinRsp rsp, Address dest) {
        Message m=new Message(dest).putHeader(this.id, new GmsHeader(GmsHeader.JOIN_RSP))
          .setBuffer(marshal(rsp)).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL);
        getDownProtocol().down(new Event(Event.MSG, m));
    }


    public void installView(View new_view) {
        installView(new_view,null);
    }


    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack. If the view is a MergeView (subclass
     * of View), then digest will be non-null and has to be set before installing the view.
     */
    public synchronized void installView(View new_view, Digest digest) {
        ViewId vid=new_view.getViewId();
        List<Address> mbrs=new_view.getMembers();
        ltime=Math.max(vid.getId(), ltime);  // compute the logical time, regardless of whether the view is accepted

        // Discards view with id lower than or equal to our own. Will be installed without check if it is the first view
        if(view != null && vid.compareToIDs(view.getViewId()) <= 0)
            return;

        /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
           This ensures that messages sent in view V1 are only received by members of V1 */
        if(!mbrs.contains(local_addr)) {
            if(log_view_warnings)
                log.warn("%s: not member of view %s; discarding it", local_addr, new_view.getViewId());
            return;
        }

        if(digest != null) {
            if(new_view instanceof MergeView)
                mergeDigest(digest);
            else
                setDigest(digest);
        }

        log.debug("%s: installing view %s", local_addr, new_view);

        Event view_event;
        synchronized(members) {
            view=new_view; // new View(new_view.getVid(), new_view.getMembers());
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
                for(Address addr: mbrs)
                    if(!prev_members.contains(addr))
                        prev_members.add(addr);
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
            prev_views.add(new Date() + ": " + new_view);
        }
    }


    protected Address determineCoordinator() {
        synchronized(members) {
            return members.size() > 0? members.elementAt(0) : null;
        }
    }

    protected static View createDeltaView(final View current_view, final View next_view) {
        final ViewId current_view_id=current_view.getViewId();
        final ViewId next_view_id=next_view.getViewId();
        Address[][] diff=View.diff(current_view, next_view);
        return new DeltaView(next_view_id, current_view_id, diff[1], diff[0]);
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
        down_prot.down(new Event(Event.MERGE_DIGEST,d));
    }


    /** Grabs the current digest from NAKACK{2} */
    public Digest getDigest() {
        return (Digest)down_prot.down(Event.GET_DIGEST_EVT);
    }

    boolean startFlush(View view) {
       return _startFlush(view, 4, true, 1000L, 5000L);
   }

   boolean startFlush(View view, int maxAttempts, long floor, long ceiling) {
       return _startFlush(view, maxAttempts, true, floor, ceiling);
   }

   protected boolean _startFlush(final View new_view, int maxAttempts, boolean resumeIfFailed, long randomFloor, long randomCeiling) {
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
                   if (attemptCount > 0)
                       Util.sleepRandom(randomFloor, randomCeiling);
                   try {
                       up_prot.up(new Event(Event.SUSPEND, new ArrayList<>(new_view.getMembers())));
                       successfulFlush = true;
                       break;
                   } catch (Exception e) {
                       attemptCount++;
                   }
               }

               if(successfulFlush) {
                   if(log.isTraceEnabled())
                       log.trace(local_addr + ": successful GMS flush by coordinator");
               }
               else {
                  if (resumeIfFailed) {
                     up(new Event(Event.RESUME, new ArrayList<>(new_view.getMembers())));
                  }
                  if (log.isWarnEnabled())
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
                final Message msg=(Message)evt.getArg();
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
                        JoinRsp join_rsp=readJoinRsp(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(join_rsp != null)
                            impl.handleJoinResponse(join_rsp);
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
                        Tuple<View,Digest> tuple=readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(tuple == null)
                            return null;
                        View new_view=tuple.getVal1();
                        if(new_view == null)
                            return null;

                        // Discards view with id lower than or equal to our own. Will be installed without check if it is the first view
                        ViewId viewId = this.getViewId();
                        if (viewId != null && new_view.getViewId().compareToIDs(viewId) <= 0)
                            return null;
                        if(new_view instanceof DeltaView) {
                            try {
                                log.trace("%s: received delta view %s", local_addr, new_view);
                                new_view=createViewFromDeltaView(view,(DeltaView)new_view);
                            }
                            catch(Throwable t) {
                                if(view != null)
                                    log.warn("%s: failed to create view from delta-view; dropping view: %s", local_addr, t.toString());
                                return null;
                            }
                        }
                        else
                            log.trace("%s: received full view: %s", local_addr, new_view);

                        Address coord=msg.getSrc();
                        if(!new_view.containsMember(coord)) {
                            sendViewAck(coord); // we need to send the ack first, otherwise the connection is removed
                            impl.handleViewChange(new_view, tuple.getVal2());
                        }
                        else {
                            impl.handleViewChange(new_view, tuple.getVal2());
                            sendViewAck(coord); // send VIEW_ACK to sender of view
                        }
                        break;

                    case GmsHeader.VIEW_ACK:
                        Address sender=msg.getSrc();
                        ack_collector.ack(sender);
                        return null; // don't pass further up

                    case GmsHeader.MERGE_REQ:
                        Collection<? extends Address> mbrs=readMembers(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(mbrs != null)
                            impl.handleMergeRequest(msg.getSrc(), hdr.merge_id, mbrs);
                        break;

                    case GmsHeader.MERGE_RSP:
                        tuple=readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(tuple == null)
                            return null;
                        MergeData merge_data=new MergeData(msg.getSrc(), tuple.getVal1(), tuple.getVal2(), hdr.merge_rejected);
                        log.trace("%s: got merge response from %s, merge_id=%s, merge data is %s",
                                  local_addr, msg.getSrc(), hdr.merge_id, merge_data);
                        impl.handleMergeResponse(merge_data, hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_MERGE_VIEW:
                        tuple=readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(tuple == null)
                            return null;
                        impl.handleMergeView(new MergeData(msg.getSrc(), tuple.getVal1(), tuple.getVal2()), hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_DIGEST:
                        tuple=readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(tuple == null)
                            return null;
                        Digest tmp=tuple.getVal2();
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
                        if(!members.contains(msg.getSrc()))
                            break;

                        // discard my own request:
                        if(msg.getSrc().equals(local_addr))
                            return null;

                        if(hdr.merge_id !=null && !(merger.matchMergeId(hdr.merge_id) || merger.setMergeId(null, hdr.merge_id)))
                            return null;

                        // fetch only my own digest
                        Digest digest=(Digest)down_prot.down(new Event(Event.GET_DIGEST, local_addr));
                        if(digest != null) {
                            Message get_digest_rsp=new Message(msg.getSrc())
                              .setFlag(Message.Flag.OOB,Message.Flag.INTERNAL)
                              .putHeader(this.id, new GmsHeader(GmsHeader.GET_DIGEST_RSP))
                              .setBuffer(marshal(null, digest));
                            down_prot.down(new Event(Event.MSG, get_digest_rsp));
                        }
                        break;

                    case GmsHeader.GET_DIGEST_RSP:
                        tuple=readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(tuple == null)
                            return null;
                        Digest digest_rsp=tuple.getVal2();
                        impl.handleDigestResponse(msg.getSrc(), digest_rsp);
                        break;

                    case GmsHeader.GET_CURRENT_VIEW:
                        ViewId view_id=readViewId(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(view_id != null) {
                            // check if my view-id differs from view-id:
                            ViewId my_view_id=this.view != null? this.view.getViewId() : null;
                            if(my_view_id != null && my_view_id.compareToIDs(view_id) <= 0)
                                return null; // my view-id doesn't differ from sender's view-id; no need to send view
                        }
                        // either my view-id differs from sender's view-id, or sender's view-id is null: send view
                        Message view_msg=new Message(msg.getSrc()).putHeader(id,new GmsHeader(GmsHeader.VIEW))
                          .setBuffer(marshal(view, null)).setFlag(Message.Flag.OOB,Message.Flag.INTERNAL);
                        down_prot.down(new Event(Event.MSG, view_msg));
                        break;

                    default:
                        if(log.isErrorEnabled()) log.error(Util.getMessage("GmsHeaderWithType"), hdr.type);
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

                if(state_transfer)
                    impl.joinWithStateTransfer(local_addr, use_flush);
                else
                    impl.join(local_addr, use_flush);
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
            case Event.GET_VIEW_FROM_COORD:
                Address coord=view != null? view.getCreator() : null;
                if(coord != null) {
                    ViewId view_id=view != null? view.getViewId() : null;
                    Message msg=new Message(coord).putHeader(id, new GmsHeader(GmsHeader.GET_CURRENT_VIEW))
                      .setBuffer(marshal(view_id)).setFlag(Message.Flag.OOB,Message.Flag.INTERNAL);
                    down_prot.down(new Event(Event.MSG, msg));
                }
                return null; // don't pass the event further down
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
        first_view_sent=false;
    }


    private void sendViewAck(Address dest) {
        Message view_ack=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(this.id, new GmsHeader(GmsHeader.VIEW_ACK));
        down_prot.down(new Event(Event.MSG,view_ack));
    }


    protected View createViewFromDeltaView(View current_view, DeltaView delta_view) {
        if(current_view == null || delta_view == null)
            throw new IllegalStateException("current view (" + current_view + ") or delta view (" + delta_view + ") is null");
        ViewId current_view_id=current_view.getViewId(),
          delta_ref_view_id=delta_view.getRefViewId(),
          delta_view_id=delta_view.getViewId();
        if(!current_view_id.equals(delta_ref_view_id))
            throw new IllegalStateException("the view-id of the delta view ("+delta_ref_view_id+") doesn't match the " +
                                              "current view-id ("+current_view_id+"); discarding delta view " + delta_view);
        List<Address> current_mbrs=current_view.getMembers();
        List<Address> left_mbrs=Arrays.asList(delta_view.getLeftMembers());
        List<Address> new_mbrs=Arrays.asList(delta_view.getNewMembers());


        List<Address> new_mbrship=computeNewMembership(current_mbrs,new_mbrs,left_mbrs,Collections.<Address>emptyList());
        return new View(delta_view_id, new_mbrship);
    }

    protected static boolean writeAddresses(final View view, final Digest digest) {
        return digest == null || view == null || !Arrays.equals(view.getMembersRaw(),digest.getMembersRaw());
    }

    protected static short determineFlags(final View view, final Digest digest) {
        short retval=0;
        if(view != null) {
            retval|=VIEW_PRESENT;
            if(view instanceof MergeView)
                retval|=MERGE_VIEW;
            else if(view instanceof DeltaView)
                retval|=DELTA_VIEW;
        }
        if(digest != null) retval|=DIGEST_PRESENT;
        if(writeAddresses(view, digest))  retval|=READ_ADDRS;
        return retval;
    }

    protected static Buffer marshal(final View view, final Digest digest) {
        try {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
            out.writeShort(determineFlags(view, digest));
            if(view != null)
                view.writeTo(out);

            if(digest != null)
                digest.writeTo(out, writeAddresses(view, digest));

            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }

    public static Buffer marshal(JoinRsp join_rsp) {
        return Util.streamableToBuffer(join_rsp);
    }

    protected static Buffer marshal(Collection<? extends Address> mbrs) {
        try {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
            Util.writeAddresses(mbrs, out);
            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected static Buffer marshal(final ViewId view_id) {
        try {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
            Util.writeViewId(view_id, out);
            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }


    protected JoinRsp readJoinRsp(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(JoinRsp.class, buffer, offset, length) : null;
        }
        catch(Exception ex) {
            log.error("%s: failed reading JoinRsp from message: %s", local_addr, ex);
            return null;
        }
    }

    protected Collection<? extends Address> readMembers(byte[] buffer, int offset, int length) {
        if(buffer == null) return null;
        try {
            DataInput in=new ByteArrayDataInputStream(buffer, offset, length);
            return Util.readAddresses(in, ArrayList.class);
        }
        catch(Exception ex) {
            log.error("%s: failed reading members from message: %s", local_addr, ex);
            return null;
        }
    }


    protected Tuple<View,Digest> readViewAndDigest(byte[] buffer, int offset, int length) {
        try {
            return _readViewAndDigest(buffer, offset, length);
        }
        catch(Exception ex) {
            log.error("%s: failed reading view and digest from message: %s", local_addr, ex);
            return null;
        }
    }


    public static Tuple<View,Digest> _readViewAndDigest(byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        DataInput in=new ByteArrayDataInputStream(buffer, offset, length);
        View tmp_view=null;
        Digest digest=null;
        short flags=in.readShort();

        if((flags & VIEW_PRESENT) == VIEW_PRESENT) {
            tmp_view=(flags & MERGE_VIEW) == MERGE_VIEW? new MergeView() :
              (flags & DELTA_VIEW) == DELTA_VIEW? new DeltaView() :
                new View();
            tmp_view.readFrom(in);
        }

        if((flags & DIGEST_PRESENT) == DIGEST_PRESENT) {
            if((flags & READ_ADDRS) == READ_ADDRS) {
                digest=new Digest();
                digest.readFrom(in);
            }
            else {
                digest=new Digest(tmp_view.getMembersRaw());
                digest.readFrom(in,false);
            }
        }
        return new Tuple<>(tmp_view, digest);
    }

    protected ViewId readViewId(byte[] buffer, int offset, int length) {
        if(buffer == null) return null;
        try {
            DataInput in=new ByteArrayDataInputStream(buffer, offset, length);
            return Util.readViewId(in);
        }
        catch(Exception ex) {
            log.error("%s: failed reading ViewId from message: %s", local_addr, ex);
            return null;
        }
    }

    /* --------------------------- End of Private Methods ------------------------------- */

    public static class DefaultMembershipPolicy implements MembershipChangePolicy {

        /**
         * Takes the existing membership list and removes suspected and left members, then adds new
         * members to the end of the list
         * @param current_members The list of current members. Guaranteed to be non-null (but may be empty)
         * @param joiners The joining members. Guaranteed to be non-null (but may be empty)
         * @param leavers Members that are leaving. Guaranteed to be non-null (but may be empty)
         * @param suspects Members which are suspected. Guaranteed to be non-null (but may be empty)
         * @return The new membership. Needs to be non-null and cannot contain duplicates
         */
        public List<Address> getNewMembership(final Collection<Address> current_members, final Collection<Address> joiners,
                                              final Collection<Address> leavers, final Collection<Address> suspects) {
            Membership mbrs=new Membership(current_members).remove(leavers).remove(suspects).add(joiners);
            return mbrs.getMembers();
        }


        /**
         * Old default implementation for a merge. Adds all members into a list, sorts the list and returns it
         * @param subviews A list of membership lists, e.g. [{A,B,C}, {M,N,O,P}, {X,Y,Z}]. This is a merge between
         *                 3 subviews. Guaranteed to be non-null (but may be empty)
         * @return The new membership. Needs to be non-null and cannot contain duplicates
         */
        public static List<Address> getNewMembershipOld(final Collection<Collection<Address>> subviews) {
            Membership mbrs=new Membership();
            for(Collection<Address> subview: subviews)
                mbrs.add(subview);
            return mbrs.sort().getMembers();
        }

        /**
         * Default implementation for a merge. Picks the new coordinator among the coordinators of the old subviews
         * by getting all coords, sorting them and picking the first. Then the coord is added to the new list, and
         * all subviews are subsequently added.<p/>
         * Tries to minimize coordinatorship moving around between different members
         * @param subviews A list of membership lists, e.g. [{A,B,C}, {M,N,O,P}, {X,Y,Z}]. This is a merge between
         *                 3 subviews. Guaranteed to be non-null (but may be empty)
         * @return The new membership. Needs to be non-null and cannot contain duplicates
         */
        public List<Address> getNewMembership(final Collection<Collection<Address>> subviews) {
            Membership coords=new Membership();
            Membership new_mbrs=new Membership();

            // add the coord of each subview
            for(Collection<Address> subview: subviews)
                if(!subview.isEmpty())
                    coords.add(subview.iterator().next());

            coords.sort();

            // pick the first coord of the sorted list as the new coord
            new_mbrs.add(coords.elementAt(0));

            // add all other members in the order in which they occurred in their subviews - dupes are not added
            for(Collection<Address> subview: subviews)
                new_mbrs.add(subview);

            return new_mbrs.getMembers();
        }
    }


    public static class GmsHeader extends Header {
        public static final byte JOIN_REQ                     =  1;
        public static final byte JOIN_RSP                     =  2;
        public static final byte LEAVE_REQ                    =  3;
        public static final byte LEAVE_RSP                    =  4;
        public static final byte VIEW                         =  5;
        public static final byte MERGE_REQ                    =  6;
        public static final byte MERGE_RSP                    =  7;
        public static final byte INSTALL_MERGE_VIEW           =  8;
        public static final byte CANCEL_MERGE                 =  9;
        public static final byte VIEW_ACK                     = 10;
        public static final byte JOIN_REQ_WITH_STATE_TRANSFER = 11;
        public static final byte INSTALL_MERGE_VIEW_OK        = 12;
        public static final byte GET_DIGEST_REQ               = 13;
        public static final byte GET_DIGEST_RSP               = 14;
        public static final byte INSTALL_DIGEST               = 15;
        public static final byte GET_CURRENT_VIEW             = 16;

        public static final short JOIN_RSP_PRESENT = 1 << 1;
        public static final short MERGE_ID_PRESENT = 1 << 2;
        public static final short USE_FLUSH        = 1 << 3;
        public static final short MERGE_REJECTED   = 1 << 4;


        protected byte    type;
        protected Address mbr;                  // used when type=JOIN_REQ or LEAVE_REQ
        protected MergeId merge_id;             // used when type=MERGE_REQ or MERGE_RSP or INSTALL_MERGE_VIEW or CANCEL_MERGE
        protected boolean useFlushIfPresent;    // used when type=JOIN_REQ
        protected boolean merge_rejected=false; // used when type=MERGE_RSP


        public GmsHeader() { // used for Externalization
        }

        public GmsHeader(byte type) {
            this.type=type;
        }

        /** Used for JOIN_REQ or LEAVE_REQ header */
        public GmsHeader(byte type, Address mbr, boolean useFlushIfPresent) {
            this.type=type;
            this.mbr=mbr;
            this.useFlushIfPresent = useFlushIfPresent;
        }

        public GmsHeader(byte type, Address mbr) {
            this(type,mbr,true);
        }


        public byte      getType()                                {return type;}
        public GmsHeader mbr(Address mbr)                         {this.mbr=mbr; return this;}
        public GmsHeader mergeId(MergeId merge_id)                {this.merge_id=merge_id; return this;}
        public GmsHeader mergeRejected(boolean flag)              {this.merge_rejected=flag; return this;}
        public Address   getMember()                              {return mbr;}
        public MergeId   getMergeId()                             {return merge_id;}
        public void      setMergeId(MergeId merge_id)             {this.merge_id=merge_id;}
        public boolean   isMergeRejected()                        {return merge_rejected;}
        public void      setMergeRejected(boolean merge_rejected) {this.merge_rejected=merge_rejected;}



        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            short flags=determineFlags();
            out.writeShort(flags);
            Util.writeAddress(mbr, out);
            if(merge_id != null)
                merge_id.writeTo(out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            short flags=in.readShort();
            mbr=Util.readAddress(in);
            if((flags & MERGE_ID_PRESENT) == MERGE_ID_PRESENT) {
                merge_id=new MergeId();
                merge_id.readFrom(in);
            }
            merge_rejected=(flags & MERGE_REJECTED) == MERGE_REJECTED;
            useFlushIfPresent=(flags & USE_FLUSH) == USE_FLUSH;
        }

        public int size() {
            int retval=Global.BYTE_SIZE  // type
              + Global.SHORT_SIZE       // flags
              + Util.size(mbr);
            if(merge_id != null)
                retval+=merge_id.size();
            return retval;
        }

        protected short determineFlags() {
            short retval=0;
            if(merge_id != null)  retval|=MERGE_ID_PRESENT;
            if(useFlushIfPresent) retval|=USE_FLUSH;
            if(merge_rejected)    retval|=MERGE_REJECTED;
            return retval;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder("GmsHeader[").append(type2String(type) + ']');
            switch(type) {
                case JOIN_REQ:
                case LEAVE_REQ:
                case GET_DIGEST_REQ:
                    sb.append(": mbr=" + mbr);
                    break;

                case MERGE_REQ:
                    sb.append(": merge_id=" + merge_id);
                    break;

                case MERGE_RSP:
                    sb.append("merge_id=" + merge_id);
                    if(merge_rejected) sb.append(", merge_rejected=" + merge_rejected);
                    break;

                case CANCEL_MERGE:
                    sb.append(", merge_id=" + merge_id);
                    break;
            }
            return sb.toString();
        }


        public static String type2String(int type) {
            switch(type) {
                case JOIN_REQ:                     return "JOIN_REQ";
                case JOIN_RSP:                     return "JOIN_RSP";
                case LEAVE_REQ:                    return "LEAVE_REQ";
                case LEAVE_RSP:                    return "LEAVE_RSP";
                case VIEW:                         return "VIEW";
                case MERGE_REQ:                    return "MERGE_REQ";
                case MERGE_RSP:                    return "MERGE_RSP";
                case INSTALL_MERGE_VIEW:           return "INSTALL_MERGE_VIEW";
                case CANCEL_MERGE:                 return "CANCEL_MERGE";
                case VIEW_ACK:                     return "VIEW_ACK";
                case JOIN_REQ_WITH_STATE_TRANSFER: return "JOIN_REQ_WITH_STATE_TRANSFER";
                case INSTALL_MERGE_VIEW_OK:        return "INSTALL_MERGE_VIEW_OK";
                case GET_DIGEST_REQ:               return "GET_DIGEST_REQ";
                case GET_DIGEST_RSP:               return "GET_DIGEST_RSP";
                case INSTALL_DIGEST:               return "INSTALL_DIGEST";
                case GET_CURRENT_VIEW:             return "GET_CURRENT_VIEW";
                default:                           return "<unknown>";
            }
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
        private final BoundedList<String>   history=new BoundedList<>(20);

        /** Current Resumer task */
        private Future<?>                   resumer;


        synchronized void add(Request req) {
            if(suspended) {
                log.trace("%s: queue is suspended; request %s is discarded",local_addr,req);
                return;
            }
            start();
            try {
                queue.add(req);
                history.add(new Date() + ": " + req.toString());
            }
            catch(QueueClosedException e) {
                log.trace("%s: queue is closed; request %s is discarded", local_addr, req);
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
            long start_time, wait_time;  // ns
            long timeout=TimeUnit.NANOSECONDS.convert(max_bundling_time, TimeUnit.MILLISECONDS);
            List<Request> requests=new LinkedList<>();
            while(Thread.currentThread().equals(thread) && !suspended) {
                try {
                    boolean keepGoing=false;
                    start_time=System.nanoTime();
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
                            wait_time=timeout - (System.nanoTime() - start_time);
                            if(wait_time > 0 && firstRequest.canBeProcessedTogether(firstRequest)) { // JGRP-1438
                                long wait_time_ms=TimeUnit.MILLISECONDS.convert(wait_time, TimeUnit.NANOSECONDS);
                                if(wait_time_ms > 0)
                                    queue.waitUntilClosed(wait_time_ms); // misnomer: waits until element has been added or q closed
                            }
                            keepGoing=queue.size() > 0 && firstRequest.canBeProcessedTogether((Request)queue.peek());
                        }
                    }
                    while(keepGoing && timeout - (System.nanoTime() - start_time) > 0);

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
