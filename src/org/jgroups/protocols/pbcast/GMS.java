package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GmsImpl.Request;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.MembershipChangePolicy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.jgroups.Message.Flag.INTERNAL;
import static org.jgroups.Message.Flag.OOB;


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

    @Property(description="Join timeout",type=AttributeType.TIME)
    protected long                      join_timeout=3000;

    @Property(description="Number of join attempts before we give up and become a singleton. 0 means 'never give up'.")
    protected int                       max_join_attempts=10;

    @Property(description="Time (in ms) to wait for another discovery round when all discovery responses were " +
      "clients. A timeout of 0 means don't wait at all.",type=AttributeType.TIME)
    protected int                       all_clients_retry_timeout=100;

    @Property(description="Max time (in ms) to wait for a LEAVE response after a LEAVE req has been sent to the coord",
      type=AttributeType.TIME)
    protected long                      leave_timeout=2000;

    @Property(description="Number of times a LEAVE request is sent to the coordinator (without receiving a LEAVE " +
      "response, before giving up and leaving anyway (failure detection will eventually exclude the left member). " +
      "A value of 0 means wait forever")
    protected int                       max_leave_attempts=10;

    @Property(description="Timeout (in ms) to complete merge",type=AttributeType.TIME)
    protected long                      merge_timeout=5000; // time to wait for all MERGE_RSPS

    @Property(description="Print local address of this member after connect. Default is true")
    protected boolean                   print_local_addr=true;

    @Property(description="Print physical address(es) on startup")
    protected boolean                   print_physical_addrs=true;

    @Property(description="If true, then GMS is allowed to send VIEW messages with delta views, otherwise " +
      "it always sends full views. See https://issues.jboss.org/browse/JGRP-1354 for details.")
    protected boolean                   use_delta_views=true;

    @Property(description="Max number of old members to keep in history. Default is 50")
    protected int                       num_prev_mbrs=50;

    @Property(description="Number of views to store in history")
    protected int                       num_prev_views=10;

    @Property(description="Time in ms to wait for all VIEW acks (0 == wait forever. Default is 2000 msec",
      type=AttributeType.TIME)
    protected long                      view_ack_collection_timeout=2000;

    @Property(description="Use flush for view changes. Default is true")
    protected boolean                   use_flush_if_present=true;

    @Property(description="Logs failures for collecting all view acks if true")
    protected boolean                   log_collect_msgs;

    @Property(description="Logs warnings for reception of views less than the current, and for views which don't include self")
    protected boolean                   log_view_warnings=true;

    @Property(description="When true, left and joined members are printed in addition to the view")
    protected boolean                   print_view_details=true;
    /* --------------------------------------------- JMX  ---------------------------------------------- */


    protected int                       num_views;

    /** Stores the last 20 views */
    protected BoundedList<String>       prev_views;


    /* --------------------------------------------- Fields ------------------------------------------------ */

    protected GmsImpl                   impl;
    protected final Lock                impl_lock=new ReentrantLock(); // synchronizes event entry into impl
    protected final Map<String,GmsImpl> impls=new HashMap<>(3);

    protected Merger                    merger; // handles merges

    protected final Leaver              leaver=new Leaver(this); // handles a member leaving the cluster

    protected Address                   local_addr;
    protected final Membership          members=new Membership();     // real membership

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
    protected final ViewHandler<Request> view_handler=
      new ViewHandler<>(this, this::process, Request::canBeProcessedTogether);

    /** To collect VIEW_ACKs from all members */
    protected final AckCollector        ack_collector=new AckCollector();

    //[JGRP-700] - FLUSH: flushing should span merge
    protected final AckCollector        merge_ack_collector=new AckCollector();

    protected boolean                   flushProtocolInStack;

    // Has this coord sent its first view since becoming coord ? Used to send a full- or delta- view */
    protected boolean                   first_view_sent;



    public GMS() {
    }

    public long    getJoinTimeout()                    {return join_timeout;}
    public GMS     setJoinTimeout(long t)              {join_timeout=t; return this;}
    public long    getLeaveTimeout()                   {return leave_timeout;}
    public GMS     setLeaveTimeout(long t)             {leave_timeout=t; return this;}
    public long    getMergeTimeout()                   {return merge_timeout;}
    public GMS     setMergeTimeout(long t)             {merge_timeout=t; return this;}
    public int     getMaxJoinAttempts()                {return max_join_attempts;}
    public GMS     setMaxJoinAttempts(int t)           {max_join_attempts=t; return this;}
    public int     getMaxLeaveAttempts()               {return max_leave_attempts;}
    public GMS     setMaxLeaveAttempts(int t)          {max_leave_attempts=t; return this;}
    public boolean printLocalAddress()                 {return print_local_addr;}
    public GMS     printLocalAddress(boolean p)        {print_local_addr=p; return this;}
    public boolean printPhysicalAddress()              {return print_physical_addrs;}
    public GMS     printPhysicalAddress(boolean p)     {print_physical_addrs=p; return this;}
    public boolean useDeltaViews()                     {return use_delta_views;}
    public GMS     useDeltaViews(boolean b)            {use_delta_views=b; return this;}
    public long    getViewAckCollectionTimeout()       {return view_ack_collection_timeout;}
    public GMS     setViewAckCollectionTimeout(long v) {this.view_ack_collection_timeout=v; return this;}
    public boolean logCollectMessages()                {return log_collect_msgs;}
    public GMS     logCollectMessages(boolean b)       {log_collect_msgs=b; return this;}
    public boolean logViewWarnings()                   {return log_view_warnings;}
    public GMS     logViewWarnings(boolean b)          {log_view_warnings=b; return this;}
    public boolean printViewDetails()                  {return print_view_details;}
    public GMS     printViewDetails(boolean p)         {print_view_details=p; return this;}
    public ViewId  getViewId()                         {return view != null? view.getViewId() : null;}
    public View    view()                              {return view;}

    /** Returns the current view and digest. Try to find a matching digest twice (if not found on the first try) */
    public Tuple<View,Digest> getViewAndDigest() {
        MutableDigest digest=new MutableDigest(view.getMembersRaw()).set(getDigest());
        return digest.allSet() || digest.set(getDigest()).allSet()? new Tuple<>(view, digest) : null;
    }

    @ManagedAttribute
    public String getView()                  {return view != null? view.toString() : "null";}
    @ManagedAttribute
    public int getNumberOfViews()            {return num_views;}
    @ManagedAttribute
    public String getLocalAddress()          {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute
    public String getMembers()               {return members.toString();}
    @ManagedAttribute
    public int  getNumMembers()              {return members.size();}


    @ManagedAttribute(description="impl")
    public String getImplementation() {
        return impl == null? "null" : impl.getClass().getSimpleName();
    }

    @ManagedAttribute(description="Whether or not the current instance is the coordinator")
    public boolean isCoord() {
        return impl instanceof CoordGmsImpl;
    }

    @ManagedAttribute(description="If true, the current member is in the process of leaving")
    public boolean isLeaving()               {return leaver.leaving.get();}

    public MembershipChangePolicy getMembershipChangePolicy() {
        return membership_change_policy;
    }

    public GMS setMembershipChangePolicy(MembershipChangePolicy membership_change_policy) {
        if(membership_change_policy != null)
            this.membership_change_policy=membership_change_policy;
        return this;
    }

    @ManagedAttribute(description="Stringified version of merge_id")
    public String getMergeId() {return merger.getMergeIdAsString();}

    @ManagedAttribute(description="Is a merge currently running")
    public boolean isMergeInProgress() {return merger.isMergeInProgress();}

    /** Only used for internal testing, don't use this method ! */
    public Merger getMerger() {return merger;}

    @Property(description="The fully qualified name of a class implementing MembershipChangePolicy.")
    public GMS setMembershipChangePolicy(String classname) {
        try {
            membership_change_policy=(MembershipChangePolicy)Util.loadClass(classname, getClass()).getDeclaredConstructor().newInstance();
            return this;
        }
        catch(Throwable e) {
            throw new IllegalArgumentException("membership_change_policy could not be created", e);
        }
    }

    @ManagedOperation(description="Prints the last (max 20) MergeIds")
    public String printMergeIdHistory() {return merger.getMergeIdHistory();}

    @ManagedOperation
    public String printPreviousMembers() {
        return prev_members == null? "" : prev_members.stream().map(Object::toString).collect(Collectors.joining(", "));
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

    @ManagedOperation public void suspendViewHandler() {view_handler.suspend();}
    @ManagedOperation public void resumeViewHandler() {view_handler.resume();}


    public ViewHandler<Request> getViewHandler() {return view_handler;}

    @ManagedOperation
    public String printPreviousViews() {
        return prev_views.stream().map(Object::toString).collect(Collectors.joining("\n"));
    }

    @ManagedOperation
    public void suspect(String suspected_member) {
        if(suspected_member == null)
            return;
        Map<Address,String> contents=NameCache.getContents();
        for(Map.Entry<Address,String> entry: contents.entrySet()) {
            String logical_name=entry.getValue();
            if(Objects.equals(logical_name, suspected_member)) {
                Address suspect=entry.getKey();
                if(suspect != null)
                    up(new Event(Event.SUSPECT, Collections.singletonList(suspect)));
            }
        }
    }


    public MergeId _getMergeId() {
        return impl instanceof CoordGmsImpl? ((CoordGmsImpl)impl).getMergeId() : null;
    }

    public GMS setLogCollectMessages(boolean flag) {log_collect_msgs=flag; return this;}

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
        return Collections.singletonList(Event.IS_MERGE_IN_PROGRESS);
    }

    public void setImpl(GmsImpl new_impl) {
        impl_lock.lock();
        try {
            if(impl == new_impl)
                return;
            impl=new_impl;
        }
        finally {
            impl_lock.unlock();
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
        if(impl != null)
            impl.init();
        transport.registerProbeHandler(this);
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        leaver.reset();
        initState();
        if(impl != null) impl.start();
    }

    public void stop() {
        if(impl != null) impl.stop();
        leaver.reset();
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
     * Computes the next view. Returns a copy that has {@code leavers} and
     * {@code suspected_mbrs} removed and {@code joiners} added.
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
                joiners.stream().filter(tmp_mbr -> !joining.contains(tmp_mbr)).forEach(joining::add);

            // Update leaving list (see DESIGN for explanations)
            if(leavers != null)
                leavers.stream().filter(addr -> !leaving.contains(addr)).forEach(leaving::add);

            if(suspected_mbrs != null)
                suspected_mbrs.stream().filter(addr -> !leaving.contains(addr)).forEach(leaving::add);
            return v;
        }
    }

    /** Computes the regular membership */
    protected List<Address> computeNewMembership(final List<Address> current_members, final Collection<Address> joiners,
                                                 final Collection<Address> leavers, final Collection<Address> suspects) {
        List<Address> joiners_copy, leavers_copy, suspects_copy;
        joiners_copy=joiners == null? Collections.emptyList() : new ArrayList<>(joiners);
        leavers_copy=leavers == null? Collections.emptyList() : new ArrayList<>(leavers);
        suspects_copy=suspects == null? Collections.emptyList() : new ArrayList<>(suspects);

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
     * Broadcasts the new view and digest as VIEW messages, possibly sends JOIN-RSP messages to joiners and then
     * waits for acks from expected_acks
     * @param new_view the new view ({@link View} or {@link MergeView})
     * @param digest the digest, can be null if new_view is not a MergeView
     * @param expected_acks the members from which to wait for VIEW_ACKs (self will be excluded)
     * @param joiners the list of members to which to send the join response (jr). If null, no JOIN_RSPs will be sent
     * @param jr the {@link JoinRsp}. If null (or joiners is null), no JOIN_RSPs will be sent
     */
    public void castViewChangeAndSendJoinRsps(View new_view, Digest digest, Collection<Address> expected_acks,
                                              Collection<Address> joiners, JoinRsp jr) {

        // Send down a local TMP_VIEW event. This is needed by certain layers (e.g. NAKACK) to compute correct digest
        // in case client's next request (e.g. getState()) reaches us *before* our own view change multicast.
        // Check NAKACK's TMP_VIEW handling for details
        up_prot.up(new Event(Event.TMP_VIEW, new_view));
        down_prot.down(new Event(Event.TMP_VIEW, new_view));

        View full_view=new_view;
        if(use_delta_views && view != null && !(new_view instanceof MergeView)) {
            if(!first_view_sent) // send the first view as coord as *full* view
                first_view_sent=true;
            else
                new_view=createDeltaView(view, new_view);
        }

        Message view_change_msg=new BytesMessage().putHeader(this.id, new GmsHeader(GmsHeader.VIEW))
          .setArray(marshal(new_view, digest)).setFlag(Message.TransientFlag.DONT_LOOPBACK);
        if(new_view instanceof MergeView) // https://issues.jboss.org/browse/JGRP-1484
            view_change_msg.setFlag(Message.Flag.NO_TOTAL_ORDER);

        ack_collector.reset(expected_acks, local_addr); // exclude self, as we'll install the view locally
        long start=System.currentTimeMillis();
        impl.handleViewChange(full_view, digest); // install the view locally first
        log.trace("%s: mcasting view %s", local_addr, new_view);
        down_prot.down(view_change_msg);
        sendJoinResponses(jr, joiners);
        try {
            if(ack_collector.size() > 0) {
                ack_collector.waitForAllAcks(view_ack_collection_timeout);
                log.trace("%s: got all ACKs (%d) for view %s in %d ms",
                          local_addr, ack_collector.expectedAcks(), new_view.getViewId(), System.currentTimeMillis()-start);
            }
        }
        catch(TimeoutException e) {
            if(log_collect_msgs);
                log.warn("%s: failed to collect all ACKs (expected=%d) for view %s after %d ms, missing %d ACKs from %s",
                         local_addr, ack_collector.expectedAcks(), new_view.getViewId(), System.currentTimeMillis()-start,
                         ack_collector.size(), ack_collector.printMissing());
        }
    }



    protected void sendJoinResponses(JoinRsp jr, Collection<Address> joiners) {
        if(jr == null || joiners == null || joiners.isEmpty())
            return;

        ByteArray marshalled_jr=marshal(jr);
        for(Address joiner: joiners) {
            log.trace("%s: sending join-rsp to %s: view=%s (%d mbrs)", local_addr, joiner, jr.getView(), jr.getView().size());
            sendJoinResponse(marshalled_jr, joiner);
        }
    }

    public void sendJoinResponse(JoinRsp rsp, Address dest) {
        Message m=new BytesMessage(dest).putHeader(this.id, new GmsHeader(GmsHeader.JOIN_RSP))
          .setArray(marshal(rsp)).setFlag(OOB, INTERNAL);
        getDownProtocol().down(m);
    }

    protected void sendJoinResponse(ByteArray marshalled_rsp, Address dest) {
        Message m=new BytesMessage(dest, marshalled_rsp).putHeader(this.id, new GmsHeader(GmsHeader.JOIN_RSP))
          .setFlag(INTERNAL);
        getDownProtocol().down(m);
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

        if(log.isDebugEnabled()) {
            Address[][] diff=View.diff(view, new_view);
            log.debug("%s: installing view %s %s", local_addr, new_view,
                      print_view_details? View.printDiff(diff) : "");
        }

        Event view_event;
        boolean was_coord, is_coord;
        synchronized(members) {
            was_coord=view != null && Objects.equals(local_addr, view.getCoord());
            view=new_view;
            is_coord=Objects.equals(local_addr, view.getCoord());
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
                mbrs.stream().filter(addr -> !prev_members.contains(addr)).forEach(addr -> prev_members.add(addr));
            }

            if(is_coord) {
                if(!was_coord) // client (view = null) or participant
                    becomeCoordinator();
            }
            else {
                if(was_coord || impl instanceof ClientGmsImpl) {
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

    protected Address getCoord() {
        impl_lock.lock();
        try {
            return isCoord()? determineNextCoordinator() : determineCoordinator();
        }
        finally {
            impl_lock.unlock();
        }
    }

    protected Address determineCoordinator() {
        synchronized(members) {
            return members.size() > 0? members.elementAt(0) : null;
        }
    }

    /** Returns the second-in-line */
    protected Address determineNextCoordinator() {
        synchronized(members) {
            return members.size() > 1? members.elementAt(1) : null;
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
            return Objects.equals(new_coord, potential_new_coord);
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

               if(successfulFlush)
                   log.trace("%s: successful GMS flush by coordinator", local_addr);
               else {
                   if(resumeIfFailed)
                       up(new Event(Event.RESUME, new ArrayList<>(new_view.getMembers())));
                   log.warn("%s: GMS flush by coordinator failed", local_addr);
               }
           }
           return successfulFlush;
       } catch (Exception e) {
           return false;
       }
   }

    void stopFlush() {
        if(flushProtocolInStack) {
            log.trace("%s: sending RESUME event", local_addr);
            up_prot.up(new Event(Event.RESUME));
        }
    }

    void stopFlush(List<Address> members) {
        log.trace("%s: sending RESUME event", local_addr);
        up_prot.up(new Event(Event.RESUME,members));
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.SUSPECT:
                Object retval=up_prot.up(evt);
                // todo: change this to only accept lists in 4.1
                Collection<Address> suspects=evt.arg() instanceof Address? Collections.singletonList(evt.arg()) : evt.arg();
                Request[] suspect_reqs=new Request[suspects.size()];
                int index=0;
                for(Address mbr: suspects)
                    suspect_reqs[index++]=new Request(Request.SUSPECT, mbr);
                view_handler.add(suspect_reqs);
                ack_collector.suspect(suspects);
                merge_ack_collector.suspect(suspects);
                return retval;

            case Event.UNSUSPECT:
                impl.unsuspect(evt.getArg());
                return null;                              // discard

            case Event.MERGE:
                view_handler.add(new Request(Request.MERGE, null, evt.getArg()));
                return null;                              // don't pass up

            case Event.IS_MERGE_IN_PROGRESS:
                return merger.isMergeInProgress();
        }
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        GmsHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);

        switch(hdr.type) {
            case GmsHeader.JOIN_REQ:
                view_handler.add(new Request(Request.JOIN, hdr.mbr, null, hdr.useFlushIfPresent));
                break;
            case GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                view_handler.add(new Request(Request.JOIN_WITH_STATE_TRANSFER, hdr.mbr, null, hdr.useFlushIfPresent));
                break;
            case GmsHeader.JOIN_RSP:
                JoinRsp join_rsp=readJoinRsp(msg.getArray(), msg.getOffset(), msg.getLength());
                if(join_rsp != null)
                    impl.handleJoinResponse(join_rsp);
                break;
            case GmsHeader.LEAVE_REQ:
                if(hdr.mbr == null)
                    return null;
                view_handler.add(new Request(Request.LEAVE, hdr.mbr));
                break;
            case GmsHeader.LEAVE_RSP:
                impl.handleLeaveResponse(msg.getSrc());
                break;
            case GmsHeader.VIEW:
                Tuple<View,Digest> tuple=readViewAndDigest(msg.getArray(), msg.getOffset(), msg.getLength());
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
                        new_view=createViewFromDeltaView(view,(DeltaView)new_view);
                    }
                    catch(Throwable t) {
                        if(view != null)
                            log.trace("%s: failed to create view from delta-view; dropping view: %s", local_addr, t.toString());
                        log.trace("%s: sending request for full view to %s", local_addr, msg.getSrc());
                        Message full_view_req=new EmptyMessage(msg.getSrc())
                          .putHeader(id, new GmsHeader(GmsHeader.GET_CURRENT_VIEW)).setFlag(OOB, INTERNAL);
                        down_prot.down(full_view_req);
                        return null;
                    }
                }
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
                Collection<? extends Address> mbrs=readMembers(msg.getArray(), msg.getOffset(), msg.getLength());
                if(mbrs != null)
                    impl.handleMergeRequest(msg.getSrc(), hdr.merge_id, mbrs);
                break;

            case GmsHeader.MERGE_RSP:
                tuple=readViewAndDigest(msg.getArray(), msg.getOffset(), msg.getLength());
                if(tuple == null)
                    return null;
                MergeData merge_data=new MergeData(msg.getSrc(), tuple.getVal1(), tuple.getVal2(), hdr.merge_rejected);
                log.trace("%s: got merge response from %s, merge_id=%s, merge data is %s",
                          local_addr, msg.getSrc(), hdr.merge_id, merge_data);
                impl.handleMergeResponse(merge_data, hdr.merge_id);
                break;

            case GmsHeader.INSTALL_MERGE_VIEW:
                tuple=readViewAndDigest(msg.getArray(), msg.getOffset(), msg.getLength());
                if(tuple == null)
                    return null;
                impl.handleMergeView(new MergeData(msg.getSrc(), tuple.getVal1(), tuple.getVal2()), hdr.merge_id);
                break;

            case GmsHeader.INSTALL_DIGEST:
                tuple=readViewAndDigest(msg.getArray(), msg.getOffset(), msg.getLength());
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
                    Message get_digest_rsp=new BytesMessage(msg.getSrc()).setFlag(OOB, INTERNAL)
                      .putHeader(this.id, new GmsHeader(GmsHeader.GET_DIGEST_RSP))
                      .setArray(marshal(null, digest));
                    down_prot.down(get_digest_rsp);
                }
                break;

            case GmsHeader.GET_DIGEST_RSP:
                tuple=readViewAndDigest(msg.getArray(), msg.getOffset(), msg.getLength());
                if(tuple == null)
                    return null;
                Digest digest_rsp=tuple.getVal2();
                impl.handleDigestResponse(msg.getSrc(), digest_rsp);
                break;

            case GmsHeader.GET_CURRENT_VIEW:
                ViewId view_id=readViewId(msg.getArray(), msg.getOffset(), msg.getLength());
                if(view_id != null) {
                    // check if my view-id differs from view-id:
                    ViewId my_view_id=this.view != null? this.view.getViewId() : null;
                    if(my_view_id != null && my_view_id.compareToIDs(view_id) <= 0)
                        return null; // my view-id doesn't differ from sender's view-id; no need to send view
                }
                // either my view-id differs from sender's view-id, or sender's view-id is null: send view
                log.trace("%s: received request for full view from %s, sending view %s", local_addr, msg.getSrc(), view);
                Message view_msg=new BytesMessage(msg.getSrc()).putHeader(id,new GmsHeader(GmsHeader.VIEW))
                  .setArray(marshal(view, null)).setFlag(OOB, INTERNAL);
                down_prot.down(view_msg);
                break;

            default:
                log.error(Util.getMessage("GmsHeaderWithType"), hdr.type);
        }
        return null;  // don't pass up
    }




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
                            (physical_addr != null? ", physical address=" + physical_addr.printIpAddress() : "") +
                            "\n-------------------------------------------------------------------");
                }
                else {
                    if(log.isDebugEnabled()) {
                        PhysicalAddress physical_addr=print_physical_addrs?
                          (PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr)) : null;
                        log.debug("address=" + local_addr + ", cluster=" + evt.getArg() +
                                    (physical_addr != null? ", physical address=" + physical_addr.printIpAddress() : ""));
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
                impl.leave();
                return down_prot.down(evt); // notify the other protocols, but ignore the result

            case Event.CONFIG :
               Map<String,Object> config=evt.getArg();
               if((config != null && config.containsKey("flush_supported"))){
                 flushProtocolInStack=true;
               }
               break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.GET_VIEW_FROM_COORD:
                Address coord=view != null? view.getCreator() : null;
                if(coord != null) {
                    ViewId view_id=view != null? view.getViewId() : null;
                    Message msg=new BytesMessage(coord).putHeader(id, new GmsHeader(GmsHeader.GET_CURRENT_VIEW))
                      .setArray(marshal(view_id)).setFlag(OOB, INTERNAL);
                    down_prot.down(msg);
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

    protected void initState() {
        becomeClient();
        view=null;
        first_view_sent=false;
    }


    protected void sendViewAck(Address dest) {
        Message view_ack=new EmptyMessage(dest).setFlag(OOB, INTERNAL)
          .putHeader(this.id, new GmsHeader(GmsHeader.VIEW_ACK));
        down_prot.down(view_ack);
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


        List<Address> new_mbrship=computeNewMembership(current_mbrs,new_mbrs,left_mbrs,Collections.emptyList());
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

    protected static ByteArray marshal(final View view, final Digest digest) {
        try {
            int expected_size=Global.SHORT_SIZE;
            if(view != null)
                expected_size+=view.serializedSize();
            boolean write_addrs=writeAddresses(view, digest);
            if(digest != null)
                expected_size=(int)digest.serializedSize(write_addrs);
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size +10);
            out.writeShort(determineFlags(view, digest));
            if(view != null)
                view.writeTo(out);

            if(digest != null)
                digest.writeTo(out, write_addrs);

            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }

    public static ByteArray marshal(JoinRsp join_rsp) {
        try {
            return Util.streamableToBuffer(join_rsp);
        }
        catch(Exception e) {
            return null;
        }
    }

    protected static ByteArray marshal(Collection<? extends Address> mbrs) {
        try {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream((int)Util.size(mbrs));
            Util.writeAddresses(mbrs, out);
            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected static ByteArray marshal(final ViewId view_id) {
        try {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(Util.size(view_id));
            Util.writeViewId(view_id, out);
            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }


    protected JoinRsp readJoinRsp(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(JoinRsp::new, buffer, offset, length) : null;
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
            return Util.readAddresses(in, ArrayList::new);
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

    protected void process(Collection<Request> requests) {
        if(requests.isEmpty())
            return;
        Request firstReq=requests.iterator().next();
        switch(firstReq.type) {
            case Request.JOIN:
            case Request.JOIN_WITH_STATE_TRANSFER:
            case Request.LEAVE:
            case Request.SUSPECT:
                impl.handleMembershipChange(requests);
                break;
            case Request.COORD_LEAVE:
                impl.handleCoordLeave();
                break;
            case Request.MERGE:
                impl.merge(firstReq.views);
                break;
            default:
                log.error("request type " + firstReq.type + " is unknown; discarded");
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
            subviews.forEach(mbrs::add);
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
            // add the coord of each subview
            Membership coords=new Membership();
            subviews.stream().filter(subview -> !subview.isEmpty()).forEach(subview -> coords.add(subview.iterator().next()));

            // pick the first coord of the sorted list as the new coord
            coords.sort();
            Membership new_mbrs=new Membership().add(coords.elementAt(0));

            // add all other members in the order in which they occurred in their subviews - dupes are not added
            subviews.forEach(new_mbrs::add);
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

        public short getMagicId() {return 55;}

        public byte      getType()                                {return type;}
        public GmsHeader mbr(Address mbr)                         {this.mbr=mbr; return this;}
        public GmsHeader mergeId(MergeId merge_id)                {this.merge_id=merge_id; return this;}
        public GmsHeader mergeRejected(boolean flag)              {this.merge_rejected=flag; return this;}
        public Address   getMember()                              {return mbr;}
        public MergeId   getMergeId()                             {return merge_id;}
        public GmsHeader setMergeId(MergeId merge_id)             {this.merge_id=merge_id; return this;}
        public boolean   isMergeRejected()                        {return merge_rejected;}
        public GmsHeader setMergeRejected(boolean merge_rejected) {this.merge_rejected=merge_rejected; return this;}

        public Supplier<? extends Header> create() {return GmsHeader::new;}

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            short flags=determineFlags();
            out.writeShort(flags);
            Util.writeAddress(mbr, out);
            if(merge_id != null)
                merge_id.writeTo(out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
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

        @Override
        public int serializedSize() {
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


}
