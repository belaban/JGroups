
package org.jgroups.protocols.pbcast;


import org.apache.commons.logging.Log;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.Queue;

import java.io.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jgroups.protocols.pbcast.GmsImpl.Request;


/**
 * Group membership protocol. Handles joins/leaves/crashes (suspicions) and emits new views
 * accordingly. Use VIEW_ENFORCER on top of this layer to make sure new members don't receive
 * any messages until they are members
 * @author Bela Ban
 * @version $Id: GMS.java,v 1.129 2007/12/13 05:15:02 vlada Exp $
 */
public class GMS extends Protocol {
    private GmsImpl           impl=null;
    Address                   local_addr=null;
    final Membership          members=new Membership();     // real membership
    private final Membership  tmp_members=new Membership(); // base for computing next view

    /** Members joined but for which no view has been received yet */
    private final Vector<Address> joining=new Vector<Address>(7);

    /** Members excluded from group, but for which no view has been received yet */
    private final Vector<Address>  leaving=new Vector<Address>(7);

    View                      view=null;
    ViewId                    view_id=null;
    private long              ltime=0;
    long                      join_timeout=5000;
    long                      leave_timeout=5000;
    long                      merge_timeout=10000;           // time to wait for all MERGE_RSPS
    private final Object      impl_mutex=new Object();       // synchronizes event entry into impl
    private final Hashtable<String,GmsImpl>   impls=new Hashtable<String,GmsImpl>(3);
    private boolean           shun=false;
    boolean                   merge_leader=false;         // can I initiate a merge ?
    private boolean           print_local_addr=true;
    boolean                   disable_initial_coord=false; // can the member become a coord on startup or not ?
    /** Setting this to false disables concurrent startups. This is only used by unit testing code
     * for testing merging. To everybody else: don't change it to false ! */
    boolean                   handle_concurrent_startup=true;
    /** Whether view bundling (http://jira.jboss.com/jira/browse/JGRP-144) should be enabled or not. Setting this to
     * false forces each JOIN/LEAVE/SUPSECT request to be handled separately. By default these requests are processed
     * together if they are queued at approximately the same time */
    private boolean           view_bundling=true;
    private long              max_bundling_time=50; // 50ms max to wait for other JOIN, LEAVE or SUSPECT requests
    static final String       CLIENT="Client";
    static final String       COORD="Coordinator";
    static final String       PART="Participant";
    TimeScheduler             timer=null;

    /** Max number of old members to keep in history */
    protected int             num_prev_mbrs=50;

    /** Keeps track of old members (up to num_prev_mbrs) */
    BoundedList<Address>      prev_members=null;

    /** If we receive a JOIN request from P and P is already in the current membership, then we send back a JOIN
     * response with an error message when this property is set to true (Channel.connect() will fail). Otherwise,
     * we return the current view */
    boolean                   reject_join_from_existing_member=true;

    int                       num_views=0;

    /** Stores the last 20 views */
    BoundedList<View>         prev_views=new BoundedList<View>(20);


    /** Class to process JOIN, LEAVE and MERGE requests */
    private final ViewHandler view_handler=new ViewHandler();

    /** To collect VIEW_ACKs from all members */
    final AckCollector        ack_collector=new AckCollector();

    /** Time in ms to wait for all VIEW acks (0 == wait forever) */
    long                      view_ack_collection_timeout=2000;

    /** How long should a Resumer wait until resuming the ViewHandler */
    long                      resume_task_timeout=20000;

    boolean                   flushProtocolInStack=false;

    public static final String       name="GMS";



    public GMS() {
        initState();
    }


    public String getName() {
        return name;
    }


    public String getView() {return view_id != null? view_id.toString() : "null";}
    public int getNumberOfViews() {return num_views;}
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "[]";}
    public int getNumMembers() {return members != null? members.size() : 0;}
    public long getJoinTimeout() {return join_timeout;}
    public void setJoinTimeout(long t) {join_timeout=t;}
    /** @deprecated */
    public long getJoinRetryTimeout() {return -1;}
    /** @deprecated */
    public void setJoinRetryTimeout(long t) {}
    public boolean isShun() {return shun;}
    public void setShun(boolean s) {shun=s;}
    public String printPreviousMembers() {
        StringBuilder sb=new StringBuilder();
        if(prev_members != null) {
            for(Address addr: prev_members) {
                sb.append(addr).append("\n");
            }
        }
        return sb.toString();
    }

    public long getViewAckCollectionTimeout() {
        return view_ack_collection_timeout;
    }

    public void setViewAckCollectionTimeout(long view_ack_collection_timeout) {
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

    public int viewHandlerSize() {return view_handler.size();}
    public boolean isViewHandlerSuspended() {return view_handler.suspended();}
    public String dumpViewHandlerQueue() {
        return view_handler.dumpQueue();
    }
    public String dumpViewHandlerHistory() {
        return view_handler.dumpHistory();
    }
    public void suspendViewHandler() {
        view_handler.suspend(null);
    }
    public void resumeViewHandler() {
        view_handler.resumeForce();
    }

    Log getLog() {return log;}

    ViewHandler getViewHandler() {return view_handler;}

    public String printPreviousViews() {
        StringBuilder sb=new StringBuilder();
        for(View view: prev_views) {
            sb.append(view).append("\n");
        }
        return sb.toString();
    }

    public boolean isCoordinator() {
        Address coord=determineCoordinator();
        return coord != null && local_addr != null && local_addr.equals(coord);
    }


    public void resetStats() {
        super.resetStats();
        num_views=0;
        prev_views.clear();
    }


    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval=new Vector<Integer>(3);
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.SET_DIGEST));
        retval.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return retval;
    }

    public void setImpl(GmsImpl new_impl) {
        synchronized(impl_mutex) {
            if(impl == new_impl) // superfluous
                return;
            impl=new_impl;
            if(log.isDebugEnabled()) {
                String msg=(local_addr != null? local_addr.toString()+" " : "") + "changed role to " + new_impl.getClass().getName();
                log.debug(msg);
            }
        }
    }


    public GmsImpl getImpl() {
        return impl;
    }


    public void init() throws Exception {
        prev_members=new BoundedList<Address>(num_prev_mbrs);
        timer=stack != null? stack.timer : null;
        if(timer == null)
            throw new Exception("GMS.init(): timer is null");
        if(impl != null)
            impl.init();
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


    /**
     * Computes the next view. Returns a copy that has <code>old_mbrs</code> and
     * <code>suspected_mbrs</code> removed and <code>new_mbrs</code> added.
     */
    public View getNextView(Collection new_mbrs, Collection old_mbrs, Collection suspected_mbrs) {
        Vector<Address> mbrs;
        long vid;
        View v;
        Membership tmp_mbrs;
        Address tmp_mbr;

        synchronized(members) {
            if(view_id == null) {
                log.error("view_id is null");
                return null; // this should *never* happen !
            }
            vid=Math.max(view_id.getId(), ltime) + 1;
            ltime=vid;
            tmp_mbrs=tmp_members.copy();  // always operate on the temporary membership
            tmp_mbrs.remove(suspected_mbrs);
            tmp_mbrs.remove(old_mbrs);
            tmp_mbrs.add(new_mbrs);
            mbrs=tmp_mbrs.getMembers();
            v=new View(local_addr, vid, mbrs);

            // Update membership (see DESIGN for explanation):
            tmp_members.set(mbrs);

            // Update joining list (see DESIGN for explanation)
            if(new_mbrs != null) {
                for(Iterator it=new_mbrs.iterator(); it.hasNext();) {
                    tmp_mbr=(Address)it.next();
                    if(!joining.contains(tmp_mbr))
                        joining.addElement(tmp_mbr);
                }
            }

            // Update leaving list (see DESIGN for explanations)
            if(old_mbrs != null) {
                for(Iterator it=old_mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    if(!leaving.contains(addr))
                        leaving.add(addr);
                }
            }
            if(suspected_mbrs != null) {
                for(Iterator it=suspected_mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    if(!leaving.contains(addr))
                        leaving.add(addr);
                }
            }
            return v;
        }
    }


    /**
     Compute a new view, given the current view, the new members and the suspected/left
     members. Then simply mcast the view to all members. This is different to the VS GMS protocol,
     in which we run a FLUSH protocol which tries to achive consensus on the set of messages mcast in
     the current view before proceeding to install the next view.

     The members for the new view are computed as follows:
     <pre>
                   existing          leaving        suspected          joining

     1. new_view      y                 n               n                 y
     2. tmp_view      y                 y               n                 y
     (view_dest)
     </pre>

     <ol>
     <li>
     The new view to be installed includes the existing members plus the joining ones and
     excludes the leaving and suspected members.
     <li>
     A temporary view is sent down the stack as an <em>event</em>. This allows the bottom layer
     (e.g. UDP or TCP) to determine the members to which to send a multicast message. Compared
     to the new view, leaving members are <em>included</em> since they have are waiting for a
     view in which they are not members any longer before they leave. So, if we did not set a
     temporary view, joining members would not receive the view (signalling that they have been
     joined successfully). The temporary view is essentially the current view plus the joining
     members (old members are still part of the current view).
     </ol>
     */
    public void castViewChange(Vector new_mbrs, Vector old_mbrs, Vector suspected_mbrs) {
        View new_view;

        // next view: current mbrs + new_mbrs - old_mbrs - suspected_mbrs
        new_view=getNextView(new_mbrs, old_mbrs, suspected_mbrs);
        castViewChange(new_view, null);
    }


    public void castViewChange(View new_view, Digest digest) {
        castViewChangeWithDest(new_view, digest, null);
    }


    /**
     * Broadcasts the new view and digest, and waits for acks from all members in the list given as argument.
     * If the list is null, we take the members who are part of new_view
     * @param new_view
     * @param digest
     * @param members
     */
    public void castViewChangeWithDest(View new_view, Digest digest, List<Address> members) {
        Message   view_change_msg;
        GmsHeader hdr;
        long      start, stop;
        ViewId    vid=new_view.getVid();
        int       size=-1;

        if(members == null || members.isEmpty())
            members=new_view.getMembers();

        if(log.isTraceEnabled())
            log.trace("mcasting view {" + new_view + "} (" + new_view.size() + " mbrs)\n");

        start=System.currentTimeMillis();
        view_change_msg=new Message(); // bcast to all members
        hdr=new GmsHeader(GmsHeader.VIEW, new_view);
        hdr.my_digest=digest;
        view_change_msg.putHeader(name, hdr);

        ack_collector.reset(vid, members);
        size=ack_collector.size();
        
        // Send down a local TMP_VIEW event. This is needed by certain layers (e.g. NAKACK) to compute correct digest
        // in case client's next request (e.g. getState()) reaches us *before* our own view change multicast.
        // Check NAKACK's TMP_VIEW handling for details   
        down_prot.up(new Event(Event.TMP_VIEW, new_view));
        down_prot.down(new Event(Event.TMP_VIEW, new_view));
        down_prot.down(new Event(Event.MSG, view_change_msg));

        try {
            ack_collector.waitForAllAcks(view_ack_collection_timeout);
            stop=System.currentTimeMillis();
            if(log.isTraceEnabled())
                log.trace("received all ACKs (" + size + ") for " + vid + " in " + (stop-start) + "ms");
        }
        catch(TimeoutException e) {
            log.warn("failed to collect all ACKs (" + size + ") for view " + new_view + " after " + view_ack_collection_timeout +
                    "ms, missing ACKs from " + ack_collector.printMissing() + " (received=" + ack_collector.printReceived() +
                    "), local_addr=" + local_addr);
        }
    }


    public void installView(View new_view) {
        installView(new_view, null);
    }


    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack. If the view is a MergeView (subclass
     * of View), then digest will be non-null and has to be set before installing the view.
     */
    public void installView(View new_view, Digest digest) {
        Address coord;
        int rc;
        ViewId vid=new_view.getVid();
        Vector mbrs=new_view.getMembers();

        // Discards view with id lower than our own. Will be installed without check if first view
        if(view_id != null) {
            rc=vid.compareTo(view_id);
            if(rc <= 0) {
                if(log.isWarnEnabled() && rc < 0) // only scream if view is smaller, silently discard same views
                    log.warn("[" + local_addr + "] received view < current view;" +
                            " discarding it (current vid: " + view_id + ", new vid: " + vid + ')');
                return;
            }
        }

        if(digest != null)
            mergeDigest(digest);

        if(log.isDebugEnabled()) log.debug("[local_addr=" + local_addr + "] view is " + new_view);
        if(stats) {
            num_views++;
            prev_views.add(new_view);
        }

        ack_collector.handleView(new_view);

        ltime=Math.max(vid.getId(), ltime);  // compute Lamport logical time

        /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
        This ensures that messages sent in view V1 are only received by members of V1 */
        if(checkSelfInclusion(mbrs) == false) {
            // only shun if this member was previously part of the group. avoids problem where multiple
            // members (e.g. X,Y,Z) join {A,B} concurrently, X is joined first, and Y and Z get view
            // {A,B,X}, which would cause Y and Z to be shunned as they are not part of the membership
            // bela Nov 20 2003
            if(shun && local_addr != null && prev_members.contains(local_addr)) {
                if(log.isWarnEnabled())
                    log.warn("I (" + local_addr + ") am not a member of view " + new_view +
                            ", shunning myself and leaving the group (prev_members are " + prev_members +
                            ", current view is " + view + ")");
                if(impl != null)
                    impl.handleExit();
                up_prot.up(new Event(Event.EXIT));
            }
            else {
                if(log.isWarnEnabled()) log.warn("I (" + local_addr + ") am not a member of view " + new_view + "; discarding view");
            }
            return;
        }

        synchronized(members) {   // serialize access to views
            // assign new_view to view_id
            if(new_view instanceof MergeView)
                view=new View(new_view.getVid(), new_view.getMembers());
            else
                view=new_view;
            view_id=vid.copy();

            // Set the membership. Take into account joining members
            if(mbrs != null && !mbrs.isEmpty()) {
                members.set(mbrs);
                tmp_members.set(members);
                joining.removeAll(mbrs);  // remove all members in mbrs from joining
                // remove all elements from 'leaving' that are not in 'mbrs'
                leaving.retainAll(mbrs);

                tmp_members.add(joining);    // add members that haven't yet shown up in the membership
                tmp_members.remove(leaving); // remove members that haven't yet been removed from the membership

                // add to prev_members
                for(Iterator it=mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    if(!prev_members.contains(addr))
                        prev_members.add(addr);
                }
            }

            // Send VIEW_CHANGE event up and down the stack:
            Event view_event=new Event(Event.VIEW_CHANGE, new_view);
            // changed order of passing view up and down (http://jira.jboss.com/jira/browse/JGRP-347)
            // changed it back (bela Sept 4 2007): http://jira.jboss.com/jira/browse/JGRP-564
            down_prot.down(view_event); // needed e.g. by failure detector or UDP
            up_prot.up(view_event);


            coord=determineCoordinator();
            // if(coord != null && coord.equals(local_addr) && !(coord.equals(vid.getCoordAddress()))) {
            // changed on suggestion by yaronr and Nicolas Piedeloupe
            if(coord != null && coord.equals(local_addr) && !haveCoordinatorRole()) {
                becomeCoordinator();
            }
            else {
                if(haveCoordinatorRole() && !local_addr.equals(coord))
                    becomeParticipant();
            }
        }
    }


    protected Address determineCoordinator() {
        synchronized(members) {
            return members != null && members.size() > 0? (Address)members.elementAt(0) : null;
        }
    }


    /** Checks whether the potential_new_coord would be the new coordinator (2nd in line) */
    protected boolean wouldBeNewCoordinator(Address potential_new_coord) {
        Address new_coord;

        if(potential_new_coord == null) return false;

        synchronized(members) {
            if(members.size() < 2) return false;
            new_coord=(Address)members.elementAt(1);  // member at 2nd place
            return new_coord != null && new_coord.equals(potential_new_coord);
        }
    }


    /** Returns true if local_addr is member of mbrs, else false */
    protected boolean checkSelfInclusion(Vector mbrs) {
        Object mbr;
        if(mbrs == null)
            return false;
        for(int i=0; i < mbrs.size(); i++) {
            mbr=mbrs.elementAt(i);
            if(mbr != null && local_addr.equals(mbr))
                return true;
        }
        return false;
    }


    public View makeView(Vector<Address> mbrs) {
        Address coord=null;
        long id=0;

        if(view_id != null) {
            coord=view_id.getCoordAddress();
            id=view_id.getId();
        }
        return new View(coord, id, mbrs);
    }


    public static View makeView(Vector<Address> mbrs, ViewId vid) {
        Address coord=null;
        long id=0;

        if(vid != null) {
            coord=vid.getCoordAddress();
            id=vid.getId();
        }
        return new View(coord, id, mbrs);
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

    boolean startFlush(View new_view){           	  
    	return (Boolean) up_prot.up(new Event(Event.SUSPEND, new_view));
    }

    void stopFlush() {
       
        if(log.isDebugEnabled()){
            log.debug("sending RESUME event");
        }
        up_prot.up(new Event(Event.RESUME));
    }


    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                GmsHeader hdr=(GmsHeader)msg.getHeader(name);
                if(hdr == null)
                    break;
                switch(hdr.type) {
                    case GmsHeader.JOIN_REQ:
                        view_handler.add(new Request(Request.JOIN, hdr.mbr, false, null));
                        break;
                    case GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                        view_handler.add(new Request(Request.JOIN_WITH_STATE_TRANSFER, hdr.mbr, false, null));
                        break;    
                    case GmsHeader.JOIN_RSP:
                        impl.handleJoinResponse(hdr.join_rsp);
                        break;
                    case GmsHeader.LEAVE_REQ:
                        if(log.isDebugEnabled())
                            log.debug("received LEAVE_REQ for " + hdr.mbr + " from " + msg.getSrc());
                        if(hdr.mbr == null) {
                            if(log.isErrorEnabled()) log.error("LEAVE_REQ's mbr field is null");
                            return null;
                        }
                        view_handler.add(new Request(Request.LEAVE, hdr.mbr, false, null));
                        break;
                    case GmsHeader.LEAVE_RSP:
                        impl.handleLeaveResponse();
                        break;
                    case GmsHeader.VIEW:
                        View new_view=hdr.view;
                        if(new_view == null) {
                            if(log.isErrorEnabled()) log.error("[VIEW]: view == null");
                            return null;
                        }

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
                        down_prot.down(new Event(Event.SUSPEND_STABLE, 20000)); 
                        
                        //[JGRP-524] - FLUSH and merge: flush doesn't wrap entire merge process
                        if(flushProtocolInStack) {
                           View v=new View(view_id.copy(), members.getMembers());
                           boolean successfulFlush = startFlush(v);
                           if (successfulFlush){
                               if(log.isTraceEnabled())
                                  log.trace("Successful flush for merge from" + getLocalAddress());
                           }
                           else {
                               if(log.isWarnEnabled())
                                  log.warn("Flush for merge from " + getLocalAddress() + " failed");
                           }                         
                        }                                                                                                                   
                        impl.handleMergeRequest(msg.getSrc(), hdr.merge_id);
                        break;

                    case GmsHeader.MERGE_RSP:
                        MergeData merge_data=new MergeData(msg.getSrc(), hdr.view, hdr.my_digest);
                        merge_data.merge_rejected=hdr.merge_rejected;
                        impl.handleMergeResponse(merge_data, hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_MERGE_VIEW:
                        impl.handleMergeView(new MergeData(msg.getSrc(), hdr.view, hdr.my_digest), hdr.merge_id);
                        down_prot.down(new Event(Event.RESUME_STABLE));
                        break;

                    case GmsHeader.CANCEL_MERGE:
                        //[JGRP-524] - FLUSH and merge: flush doesn't wrap entire merge process
                        if(flushProtocolInStack){                            
                            stopFlush();
                        }
                        impl.handleMergeCancelled(hdr.merge_id);
                        down_prot.down(new Event(Event.RESUME_STABLE));
                        break;

                    default:
                        if(log.isErrorEnabled()) log.error("GmsHeader with type=" + hdr.type + " not known");
                }
                return null;  // don't pass up

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                if(print_local_addr) {
                    System.out.println("\n-------------------------------------------------------\n" +
                                       "GMS: address is " + local_addr +
                                       "\n-------------------------------------------------------");
                }
                break;                               // pass up

            case Event.SUSPECT:
                Address suspected=(Address)evt.getArg();
                view_handler.add(new Request(Request.SUSPECT, suspected, true, null));
                ack_collector.suspect(suspected);
                break;                               // pass up

            case Event.UNSUSPECT:
                impl.unsuspect((Address)evt.getArg());
                return null;                              // discard

            case Event.MERGE:
                view_handler.add(new Request(Request.MERGE, null, false, (Vector<Address>)evt.getArg()));
                return null;                              // don't pass up
        }

        if(impl.handleUpEvent(evt))
            return up_prot.up(evt);
        return null;
    }



    

    /**
     This method is overridden to avoid hanging on getDigest(): when a JOIN is received, the coordinator needs
     to retrieve the digest from the NAKACK layer. It therefore sends down a GET_DIGEST event, to which the NAKACK layer
     responds with a GET_DIGEST_OK event.<p>
     However, the GET_DIGEST_OK event will not be processed because the thread handling the JOIN request won't process
     the GET_DIGEST_OK event until the JOIN event returns. The receiveUpEvent() method is executed by the up-handler
     thread of the lower protocol and therefore can handle the event. All we do here is unblock the mutex on which
     JOIN is waiting, allowing JOIN to return with a valid digest. The GET_DIGEST_OK event is then discarded, because
     it won't be processed twice.
     */
//    public void receiveUpEvent(Event evt) {
//        switch(evt.getType()) {
//            case Event.GET_DIGEST_OK:
//                digest_promise.setResult(evt.getArg());
//                return; // don't pass further up
//        }
//        super.receiveUpEvent(evt);
//    }


    public Object down(Event evt) {
        Object arg=null;
        switch(evt.getType()) {            
            case Event.CONNECT:               
                down_prot.down(evt);
                if(local_addr == null)
                    if(log.isFatalEnabled()) log.fatal("[CONNECT] local_addr is null");
                try {
                    impl.join(local_addr);
                }
                catch(Throwable e) {
                    arg=e;
                }
                return arg;  // don't pass down: was already passed down
                
            case Event.CONNECT_WITH_STATE_TRANSFER:                
                down_prot.down(evt);
                if(local_addr == null)
                    if(log.isFatalEnabled()) log.fatal("[CONNECT] local_addr is null");
                try {
                    impl.joinWithStateTransfer(local_addr);
                }
                catch(Throwable e) {
                    arg=e;
                }
                return arg;  // don't pass down: was already passed down    

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
        }

        return down_prot.down(evt);
    }


    /** Setup the Protocol instance according to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("shun");
        if(str != null) {
            shun=Boolean.valueOf(str).booleanValue();
            props.remove("shun");
        }

        str=props.getProperty("merge_leader");
        if(str != null) {
            merge_leader=Boolean.valueOf(str).booleanValue();
            props.remove("merge_leader");
        }

        str=props.getProperty("print_local_addr");
        if(str != null) {
            print_local_addr=Boolean.valueOf(str).booleanValue();
            props.remove("print_local_addr");
        }

        str=props.getProperty("join_timeout");           // time to wait for JOIN
        if(str != null) {
            join_timeout=Long.parseLong(str);
            props.remove("join_timeout");
        }

        str=props.getProperty("join_retry_timeout");     // time to wait between JOINs
        if(str != null) {
            props.remove("join_retry_timeout");
            if(log.isWarnEnabled())
                log.warn("join_retry_timeout has been deprecated and its value will be ignored");
        }

        str=props.getProperty("leave_timeout");           // time to wait until coord responds to LEAVE req.
        if(str != null) {
            leave_timeout=Long.parseLong(str);
            props.remove("leave_timeout");
        }

        str=props.getProperty("merge_timeout");           // time to wait for MERGE_RSPS from subgroup coordinators
        if(str != null) {
            merge_timeout=Long.parseLong(str);
            props.remove("merge_timeout");
        }

        str=props.getProperty("digest_timeout");          // time to wait for GET_DIGEST_OK from PBCAST
        if(str != null) {
            log.warn("digest_timeout has been deprecated and its value will be ignored");
            props.remove("digest_timeout");
        }

        str=props.getProperty("view_ack_collection_timeout");
        if(str != null) {
            view_ack_collection_timeout=Long.parseLong(str);
            props.remove("view_ack_collection_timeout");
        }

        str=props.getProperty("resume_task_timeout");
        if(str != null) {
            resume_task_timeout=Long.parseLong(str);
            props.remove("resume_task_timeout");
        }

        str=props.getProperty("disable_initial_coord");
        if(str != null) {
            disable_initial_coord=Boolean.valueOf(str).booleanValue();
            props.remove("disable_initial_coord");
            if(log.isWarnEnabled())
                log.warn("disable_initial_coord has been deprecated and will be phased out by 3.0, please don't use it anymore");
        }

        str=props.getProperty("handle_concurrent_startup");
        if(str != null) {
            handle_concurrent_startup=Boolean.valueOf(str).booleanValue();
            props.remove("handle_concurrent_startup");
        }

        str=props.getProperty("num_prev_mbrs");
        if(str != null) {
            num_prev_mbrs=Integer.parseInt(str);
            props.remove("num_prev_mbrs");
        }

        str=props.getProperty("reject_join_from_existing_member");
        if(str != null) {
            reject_join_from_existing_member=Boolean.parseBoolean(str);
            props.remove("reject_join_from_existing_member");
        }
        
        str=props.getProperty("use_flush");
        if(str != null) {
            log.warn("use_flush has been deprecated and its value will be ignored");
            props.remove("use_flush");
        } 
        str=props.getProperty("flush_timeout");   
        if(str != null) {
            log.warn("flush_timeout has been deprecated and its value will be ignored");
            props.remove("flush_timeout");
        }  

        str=props.getProperty("view_bundling");
        if(str != null) {
            view_bundling=Boolean.valueOf(str).booleanValue();
            props.remove("view_bundling");
        }

        str=props.getProperty("max_bundling_time");
        if(str != null) {
            max_bundling_time=Long.parseLong(str);
            props.remove("max_bundling_time");
        }


        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }



    /* ------------------------------- Private Methods --------------------------------- */

    final void initState() {
        becomeClient();
        view_id=null;
        view=null;
    }


    private void sendViewAck(Address dest) {
        Message view_ack=new Message(dest, null, null);
        view_ack.setFlag(Message.OOB);
        GmsHeader tmphdr=new GmsHeader(GmsHeader.VIEW_ACK);
        view_ack.putHeader(name, tmphdr);
        if(log.isTraceEnabled())
            log.trace("sending VIEW_ACK to " + dest);
        down_prot.down(new Event(Event.MSG, view_ack));
    }

    /* --------------------------- End of Private Methods ------------------------------- */



    public static class GmsHeader extends Header implements Streamable {
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

        byte type=0;
        View view=null;            // used when type=VIEW or MERGE_RSP or INSTALL_MERGE_VIEW
        Address mbr=null;             // used when type=JOIN_REQ or LEAVE_REQ
        JoinRsp join_rsp=null;        // used when type=JOIN_RSP
        Digest my_digest=null;          // used when type=MERGE_RSP or INSTALL_MERGE_VIEW
        ViewId merge_id=null;        // used when type=MERGE_REQ or MERGE_RSP or INSTALL_MERGE_VIEW or CANCEL_MERGE
        boolean merge_rejected=false; // used when type=MERGE_RSP
        private static final long serialVersionUID=2369798797842183276L;


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
        public GmsHeader(byte type, Address mbr) {
            this.type=type;
            this.mbr=mbr;
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

        public String toString() {
            StringBuilder sb=new StringBuilder("GmsHeader");
            sb.append('[' + type2String(type) + ']');
            switch(type) {
                case JOIN_REQ:
                    sb.append(": mbr=" + mbr);
                    break;

                case JOIN_RSP:
                    sb.append(": join_rsp=" + join_rsp);
                    break;

                case LEAVE_REQ:
                    sb.append(": mbr=" + mbr);
                    break;

                case LEAVE_RSP:
                    break;

                case VIEW:
                case VIEW_ACK:
                    sb.append(": view=" + view);
                    break;

                case MERGE_REQ:
                    sb.append(": merge_id=" + merge_id);
                    break;

                case MERGE_RSP:
                    sb.append(": view=" + view + ", digest=" + my_digest + ", merge_rejected=" + merge_rejected +
                            ", merge_id=" + merge_id);
                    break;

                case INSTALL_MERGE_VIEW:
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
                default: return "<unknown>";
            }
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeObject(view);
            out.writeObject(mbr);
            out.writeObject(join_rsp);
            out.writeObject(my_digest);
            out.writeObject(merge_id);
            out.writeBoolean(merge_rejected);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            view=(View)in.readObject();
            mbr=(Address)in.readObject();
            join_rsp=(JoinRsp)in.readObject();
            my_digest=(Digest)in.readObject();
            merge_id=(ViewId)in.readObject();
            merge_rejected=in.readBoolean();
        }


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            boolean isMergeView=view != null && view instanceof MergeView;
            out.writeBoolean(isMergeView);
            Util.writeStreamable(view, out);
            Util.writeAddress(mbr, out);
            Util.writeStreamable(join_rsp, out);
            Util.writeStreamable(my_digest, out);
            Util.writeStreamable(merge_id, out); // kludge: we know merge_id is a ViewId
            out.writeBoolean(merge_rejected);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            boolean isMergeView=in.readBoolean();
            if(isMergeView)
                view=(View)Util.readStreamable(MergeView.class, in);
            else
                view=(View)Util.readStreamable(View.class, in);
            mbr=Util.readAddress(in);
            join_rsp=(JoinRsp)Util.readStreamable(JoinRsp.class, in);
            my_digest=(Digest)Util.readStreamable(Digest.class, in);
            merge_id=(ViewId)Util.readStreamable(ViewId.class, in);
            merge_rejected=in.readBoolean();
        }

        public int size() {
            int retval=Global.BYTE_SIZE *2; // type + merge_rejected

            retval+=Global.BYTE_SIZE; // presence view
            retval+=Global.BYTE_SIZE; // MergeView or View
            if(view != null)
                retval+=view.serializedSize();

            retval+=Util.size(mbr);

            retval+=Global.BYTE_SIZE; // presence of join_rsp
            if(join_rsp != null)
                retval+=join_rsp.serializedSize();

            retval+=Global.BYTE_SIZE; // presence for my_digest
            if(my_digest != null)
                retval+=my_digest.serializedSize();

            retval+=Global.BYTE_SIZE; // presence for merge_id
            if(merge_id != null)
                retval+=merge_id.serializedSize();
            return retval;
        }

    }









    /**
     * Class which processes JOIN, LEAVE and MERGE requests. Requests are queued and processed in FIFO order
     * @author Bela Ban
     * @version $Id: GMS.java,v 1.129 2007/12/13 05:15:02 vlada Exp $
     */
    class ViewHandler implements Runnable {
        volatile Thread                    thread;
        Queue                              q=new Queue(); // Queue<Request>
        boolean                            suspended=false;
        final static long                  INTERVAL=5000;
        private static final long          MAX_COMPLETION_TIME=10000;
        /** Maintains a list of the last 20 requests */
        private final BoundedList<String>  history=new BoundedList<String>(20);

        /** Map<Object,Future>. Keeps track of Resumer tasks which have not fired yet */
        private final Map<Object, Future>  resume_tasks=new HashMap<Object,Future>();
        private Object                     merge_id=null;


        void add(Request req) {
            add(req, false, false);
        }

        synchronized void add(Request req, boolean at_head, boolean unsuspend) {
            if(suspended && !unsuspend) {
                log.warn("queue is suspended; request " + req + " is discarded");
                return;
            }
            start(unsuspend);
            try {
                if(at_head)
                    q.addAtHead(req);
                else
                    q.add(req);
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
            }
            if(resume)
                resumeForce();
        }

        /**
         * Waits until the current request has been processes, then clears the queue and discards new
         * requests from now on
         */
        public synchronized void suspend(Object merge_id) {
            if(suspended)
                return;
            suspended=true;
            this.merge_id=merge_id;
            q.clear();
            waitUntilCompleted(MAX_COMPLETION_TIME);
            q.close(true);
            if(log.isTraceEnabled())
                log.trace("suspended ViewHandler");
            Resumer resumer=new Resumer(merge_id, resume_tasks, this);
            Future future=timer.schedule(resumer, resume_task_timeout, TimeUnit.MILLISECONDS);
            Future old_future=resume_tasks.put(merge_id, future);
            if(old_future != null)
                old_future.cancel(true);

        }


        public synchronized void resume(Object merge_id) {
            if(!suspended)
                return;
            boolean same_merge_id=this.merge_id != null && merge_id != null && this.merge_id.equals(merge_id);
            same_merge_id=same_merge_id || (this.merge_id == null && merge_id == null);

            if(!same_merge_id) {
                if(log.isWarnEnabled())
                    log.warn("resume(" +merge_id+ ") does not match " + this.merge_id + ", ignoring resume()");
                return;
            }
            synchronized(resume_tasks) {
                Future future=resume_tasks.get(merge_id);
                if(future != null) {
                    future.cancel(false);
                    resume_tasks.remove(merge_id);
                }
            }
            resumeForce();
        }

        public synchronized void resumeForce() {
            if(q.closed())
                q.reset();
            suspended=false;
            if(log.isTraceEnabled())
                log.trace("resumed ViewHandler");
        }

        public void run() {
            long start, stop, wait_time;
            List<Request> requests=new LinkedList<Request>();
            while(Thread.currentThread().equals(thread)) {
                requests.clear();
                try {
                    boolean keepGoing=false;
                    start=System.currentTimeMillis();
                    do {
                        Request firstRequest=(Request)q.remove(INTERVAL); // throws a TimeoutException if it runs into timeout
                        requests.add(firstRequest);
                        if(q.size() > 0) {
                            Request nextReq=(Request)q.peek();
                            keepGoing=view_bundling && firstRequest.canBeProcessedTogether(nextReq);
                        }
                        else {
                            stop=System.currentTimeMillis();
                            wait_time=max_bundling_time - (stop-start);
                            if(wait_time > 0)
                                Util.sleep(wait_time);
                            keepGoing=q.size() > 0;
                        }
                    }
                    while(keepGoing);
                    process(requests);
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

        public int size() {return q.size();}
        public boolean suspended() {return suspended;}
        public String dumpQueue() {
            StringBuilder sb=new StringBuilder();
            List v=q.values();
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
            if(log.isTraceEnabled())
                log.trace("processing " + requests);
            Request firstReq=requests.get(0);
            switch(firstReq.type) {
                case Request.JOIN:
                case Request.JOIN_WITH_STATE_TRANSFER:
                case Request.LEAVE:
                case Request.SUSPECT:                   
                    impl.handleMembershipChange(requests);
                    break;
                case Request.MERGE:
                    if(requests.size() > 1)
                        log.error("more than one MERGE request to process, ignoring the others");
                    impl.merge(firstReq.coordinators);
                    break;
                case Request.VIEW:
                    if(requests.size() > 1)
                        log.error("more than one VIEW request to process, ignoring the others");
                    
                    try {                       
                        castViewChangeWithDest(firstReq.view, firstReq.digest, firstReq.target_members);
                    }
                    finally {
                        //[JGRP-524] - FLUSH and merge: flush doesn't wrap entire merge process
                        if(flushProtocolInStack)
                            stopFlush();
                    }
                    break;
                default:
                    log.error("request " + firstReq.type + " is unknown; discarded");
            }
        }




        synchronized void start(boolean unsuspend) {
            if(q.closed())
                q.reset();
            if(unsuspend) {
                suspended=false;
                Future future;
                synchronized(resume_tasks) {
                    future=resume_tasks.remove(merge_id);
                }
                if(future != null)
                    future.cancel(true);
            }
            merge_id=null;
            if(thread == null || !thread.isAlive()) {
                thread=getProtocolStack().getThreadFactory().newThread(this, "ViewHandler");                
                thread.setDaemon(false); // thread cannot terminate if we have tasks left, e.g. when we as coord leave
                thread.start();
            }
        }

        synchronized void stop(boolean flush) {
            q.close(flush);
            synchronized(resume_tasks) {
                for(Future future: resume_tasks.values()) {
                    future.cancel(true);
                }
                resume_tasks.clear();
            }
            merge_id=null;
            // resumeForce();
        }
    }


    /**
     * Resumer is a second line of defense: when the ViewHandler is suspended, it will be resumed when the current
     * merge is cancelled, or when the merge completes. However, in a case where this never happens (this
     * shouldn't be the case !), the Resumer will nevertheless resume the ViewHandler.
     * We chose this strategy because ViewHandler is critical: if it is suspended indefinitely, we would
     * not be able to process new JOIN requests ! So, this is for peace of mind, although it most likely
     * will never be used...
     */
    static class Resumer implements Runnable {
        final Object                      token;
        final Map<Object,Future> tasks;
        final ViewHandler                 handler;


        public Resumer(final Object token, final Map<Object,Future> t, final ViewHandler handler) {
            this.token=token;
            this.tasks=t;
            this.handler=handler;
        }

        public void run() {
            boolean execute=true;
            synchronized(tasks) {
                Future future=tasks.get(token);
                if(future != null) {
                    future.cancel(false);
                    execute=true;
                }
                else {
                    execute=false;
                }
                tasks.remove(token);
            }
            if(execute) {
                handler.resume(token);
            }
        }
    }


}
