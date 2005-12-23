// $Id: GMS.java,v 1.49 2005/12/23 14:57:06 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.Queue;
import org.apache.commons.logging.Log;

import java.io.*;
import java.util.*;
import java.util.List;


/**
 * Group membership protocol. Handles joins/leaves/crashes (suspicions) and emits new views
 * accordingly. Use VIEW_ENFORCER on top of this layer to make sure new members don't receive
 * any messages until they are members.
 */
public class GMS extends Protocol {
    private GmsImpl           impl=null;
    Address                   local_addr=null;
    final Membership          members=new Membership();     // real membership
    private final Membership  tmp_members=new Membership(); // base for computing next view

    /** Members joined but for which no view has been received yet */
    private final Vector      joining=new Vector(7);

    /** Members excluded from group, but for which no view has been received yet */
    private final Vector      leaving=new Vector(7);

    View                      view=null;
    ViewId                    view_id=null;
    private long              ltime=0;
    long                      join_timeout=5000;
    long                      join_retry_timeout=2000;
    long                      leave_timeout=5000;
    private long              digest_timeout=0;              // time to wait for a digest (from PBCAST). should be fast
    long                      merge_timeout=10000;           // time to wait for all MERGE_RSPS
    private final Object      impl_mutex=new Object();       // synchronizes event entry into impl
    private final Object      digest_mutex=new Object();
    private final Promise     digest_promise=new Promise();  // holds result of GET_DIGEST event
    private final Hashtable   impls=new Hashtable(3);
    private boolean           shun=true;
    boolean                   merge_leader=false;         // can I initiate a merge ?
    private boolean           print_local_addr=true;
    boolean                   disable_initial_coord=false; // can the member become a coord on startup or not ?
    /** Setting this to false disables concurrent startups. This is only used by unit testing code
     * for testing merging. To everybody else: don't change it to false ! */
    boolean                   handle_concurrent_startup=true;
    static final String       CLIENT="Client";
    static final String       COORD="Coordinator";
    static final String       PART="Participant";
    TimeScheduler             timer=null;

    /** Max number of old members to keep in history */
    protected int             num_prev_mbrs=50;

    /** Keeps track of old members (up to num_prev_mbrs) */
    BoundedList               prev_members=null;

    int num_views=0;

    /** Stores the last 20 views */
    BoundedList               prev_views=new BoundedList(20);


    /** Class to process JOIN, LEAVE and MERGE requests */
    public final ViewHandler  view_handler=new ViewHandler();

    /** To collect VIEW_ACKs from all members */
    final AckCollector ack_collector=new AckCollector();

    /** Time in ms to wait for all VIEW acks (0 == wait forever) */
    long                      view_ack_collection_timeout=20000;

    /** How long should a Resumer wait until resuming the ViewHandler */
    long                      resume_task_timeout=20000;

    static final String       name="GMS";



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
    public long getJoinRetryTimeout() {return join_retry_timeout;}
    public void setJoinRetryTimeout(long t) {join_retry_timeout=t;}
    public boolean isShun() {return shun;}
    public void setShun(boolean s) {shun=s;}
    public String printPreviousMembers() {
        StringBuffer sb=new StringBuffer();
        if(prev_members != null) {
            for(Enumeration en=prev_members.elements(); en.hasMoreElements();) {
                sb.append(en.nextElement()).append("\n");
            }
        }
        return sb.toString();
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

    public String printPreviousViews() {
        StringBuffer sb=new StringBuffer();
        for(Enumeration en=prev_views.elements(); en.hasMoreElements();) {
            sb.append(en.nextElement()).append("\n");
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
        prev_views.removeAll();
    }


    public Vector requiredDownServices() {
        Vector retval=new Vector(3);
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
        prev_members=new BoundedList(num_prev_mbrs);
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
            prev_members.removeAll();
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
    public View getNextView(Vector new_mbrs, Vector old_mbrs, Vector suspected_mbrs) {
        Vector mbrs;
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
                for(int i=0; i < new_mbrs.size(); i++) {
                    tmp_mbr=(Address)new_mbrs.elementAt(i);
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

            if(log.isDebugEnabled()) log.debug("new view is " + v);
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
     @return View The new view
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
    public void castViewChangeWithDest(View new_view, Digest digest, java.util.List members) {
        Message   view_change_msg;
        GmsHeader hdr;
        long      start, stop;
        ViewId    vid=new_view.getVid();
        int       size=-1;

        if(members == null || members.size() == 0)
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
        passDown(new Event(Event.MSG, view_change_msg));
        try {
            ack_collector.waitForAllAcks(view_ack_collection_timeout);
            stop=System.currentTimeMillis();
            if(trace)
                log.trace("received all ACKs (" + size + ") for " + vid + " in " + (stop-start) + "ms");
        }
        catch(TimeoutException e) {
            log.warn("failed to collect all ACKs (" + size + ") for view " + vid + " after " + view_ack_collection_timeout +
                    "ms, missing ACKs from " + ack_collector.getMissing() + " (received=" + ack_collector.getReceived() +
                    "), local_addr=" + local_addr);
        }
    }


    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack. If the view is a MergeView (subclass
     * of View), then digest will be non-null and has to be set before installing the view.
     */
    public void installView(View new_view, Digest digest) {
        if(digest != null)
            mergeDigest(digest);
        installView(new_view);
    }


    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack.
     */
    public void installView(View new_view) {
        Address coord;
        int rc;
        ViewId vid=new_view.getVid();
        Vector mbrs=new_view.getMembers();

        if(log.isDebugEnabled()) log.debug("[local_addr=" + local_addr + "] view is " + new_view);
        if(stats) {
            num_views++;
            prev_views.add(new_view);
        }

        // Discards view with id lower than our own. Will be installed without check if first view
        if(view_id != null) {
            rc=vid.compareTo(view_id);
            if(rc <= 0) {
                if(log.isTraceEnabled() && rc < 0) // only scream if view is smaller, silently discard same views
                    log.trace("[" + local_addr + "] received view < current view;" +
                            " discarding it (current vid: " + view_id + ", new vid: " + vid + ')');
                return;
            }
        }

        ltime=Math.max(vid.getId(), ltime);  // compute Lamport logical time

        /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
        This ensures that messages sent in view V1 are only received by members of V1 */
        if(checkSelfInclusion(mbrs) == false) {
            // only shun if this member was previously part of the group. avoids problem where multiple
            // members (e.g. X,Y,Z) join {A,B} concurrently, X is joined first, and Y and Z get view
            // {A,B,X}, which would cause Y and Z to be shunned as they are not part of the membership
            // bela Nov 20 2003
            if(shun && local_addr != null && prev_members.contains(local_addr)) {
                if(warn)
                    log.warn("I (" + local_addr + ") am not a member of view " + new_view +
                            ", shunning myself and leaving the group (prev_members are " + prev_members +
                            ", current view is " + view + ")");
                if(impl != null)
                    impl.handleExit();
                passUp(new Event(Event.EXIT));
            }
            else {
                if(warn) log.warn("I (" + local_addr + ") am not a member of view " + new_view + "; discarding view");
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
            if(mbrs != null && mbrs.size() > 0) {
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
            Event view_event=new Event(Event.VIEW_CHANGE, new_view.clone());
            passDown(view_event); // needed e.g. by failure detector or UDP
            passUp(view_event);

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


    public View makeView(Vector mbrs) {
        Address coord=null;
        long id=0;

        if(view_id != null) {
            coord=view_id.getCoordAddress();
            id=view_id.getId();
        }
        return new View(coord, id, mbrs);
    }


    public View makeView(Vector mbrs, ViewId vid) {
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
        passDown(new Event(Event.SET_DIGEST, d));
    }


    /** Send down a MERGE_DIGEST event */
    public void mergeDigest(Digest d) {
        passDown(new Event(Event.MERGE_DIGEST, d));
    }


    /** Sends down a GET_DIGEST event and waits for the GET_DIGEST_OK response, or
     timeout, whichever occurs first */
    public Digest getDigest() {
        Digest ret=null;

        synchronized(digest_mutex) {
            digest_promise.reset();
            passDown(Event.GET_DIGEST_EVT);
            try {
                ret=(Digest)digest_promise.getResultWithTimeout(digest_timeout);
            }
            catch(TimeoutException e) {
                if(log.isErrorEnabled()) log.error("digest could not be fetched from below");
            }
            return ret;
        }
    }


    public void up(Event evt) {
        Object obj;
        Message msg;
        GmsHeader hdr;
        MergeData merge_data;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                obj=msg.getHeader(name);
                if(obj == null || !(obj instanceof GmsHeader))
                    break;
                hdr=(GmsHeader)msg.removeHeader(name);
                switch(hdr.type) {
                    case GmsHeader.JOIN_REQ:
                        view_handler.add(new Request(Request.JOIN, hdr.mbr, false, null));
                        break;
                    case GmsHeader.JOIN_RSP:
                        impl.handleJoinResponse(hdr.join_rsp);
                        break;
                    case GmsHeader.LEAVE_REQ:
                        if(log.isDebugEnabled())
                            log.debug("received LEAVE_REQ for " + hdr.mbr + " from " + msg.getSrc());
                        if(hdr.mbr == null) {
                            if(log.isErrorEnabled()) log.error("LEAVE_REQ's mbr field is null");
                            return;
                        }
                        view_handler.add(new Request(Request.LEAVE, hdr.mbr, false, null));
                        break;
                    case GmsHeader.LEAVE_RSP:
                        impl.handleLeaveResponse();
                        break;
                    case GmsHeader.VIEW:
                        if(hdr.view == null) {
                            if(log.isErrorEnabled()) log.error("[VIEW]: view == null");
                            return;
                        }

                        // send VIEW_ACK to sender of view
                        Address coord=msg.getSrc();
                        Message view_ack=new Message(coord, null, null);
                        GmsHeader tmphdr=new GmsHeader(GmsHeader.VIEW_ACK, hdr.view);
                        view_ack.putHeader(name, tmphdr);
                        passDown(new Event(Event.MSG, view_ack));
                        impl.handleViewChange(hdr.view, hdr.my_digest);
                        break;

                    case GmsHeader.VIEW_ACK:
                        Object sender=msg.getSrc();
                        ack_collector.ack(sender);
                        return; // don't pass further up

                    case GmsHeader.MERGE_REQ:
                        impl.handleMergeRequest(msg.getSrc(), hdr.merge_id);
                        break;

                    case GmsHeader.MERGE_RSP:
                        merge_data=new MergeData(msg.getSrc(), hdr.view, hdr.my_digest);
                        merge_data.merge_rejected=hdr.merge_rejected;
                        impl.handleMergeResponse(merge_data, hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_MERGE_VIEW:
                        impl.handleMergeView(new MergeData(msg.getSrc(), hdr.view, hdr.my_digest), hdr.merge_id);
                        break;

                    case GmsHeader.CANCEL_MERGE:
                        impl.handleMergeCancelled(hdr.merge_id);
                        break;

                    default:
                        if(log.isErrorEnabled()) log.error("GmsHeader with type=" + hdr.type + " not known");
                }
                return;  // don't pass up

            case Event.CONNECT_OK:     // sent by someone else, but WE are responsible for sending this !
            case Event.DISCONNECT_OK:  // dito (e.g. sent by TP layer). Don't send up the stack
                return;


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
                view_handler.add(new Request(Request.LEAVE, suspected, true, null));
                ack_collector.suspect(suspected);
                break;                               // pass up

            case Event.UNSUSPECT:
                impl.unsuspect((Address)evt.getArg());
                return;                              // discard

            case Event.MERGE:
                view_handler.add(new Request(Request.MERGE, null, false, (Vector)evt.getArg()));
                return;                              // don't pass up
        }

        if(impl.handleUpEvent(evt))
            passUp(evt);
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
    public void receiveUpEvent(Event evt) {
        switch(evt.getType()) {
            case Event.GET_DIGEST_OK:
                digest_promise.setResult(evt.getArg());
                return; // don't pass further up
        }
        super.receiveUpEvent(evt);
    }


    public void down(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT:
                passDown(evt);
                if(local_addr == null)
                    if(log.isFatalEnabled()) log.fatal("[CONNECT] local_addr is null");
                impl.join(local_addr);
                passUp(new Event(Event.CONNECT_OK));
                return;                              // don't pass down: was already passed down

            case Event.DISCONNECT:
                impl.leave((Address)evt.getArg());
                passUp(new Event(Event.DISCONNECT_OK));
                initState(); // in case connect() is called again
                break;       // pass down
        }

        if(impl.handleDownEvent(evt))
            passDown(evt);
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
            join_retry_timeout=Long.parseLong(str);
            props.remove("join_retry_timeout");
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
            digest_timeout=Long.parseLong(str);
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

        if(props.size() > 0) {
            log.error("GMS.setProperties(): the following properties are not recognized: " + props);

            return false;
        }
        return true;
    }



    /* ------------------------------- Private Methods --------------------------------- */

    void initState() {
        becomeClient();
        view_id=null;
        view=null;
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

        byte type=0;
        View view=null;            // used when type=VIEW or MERGE_RSP or INSTALL_MERGE_VIEW
        Address mbr=null;             // used when type=JOIN_REQ or LEAVE_REQ
        JoinRsp join_rsp=null;        // used when type=JOIN_RSP
        Digest my_digest=null;          // used when type=MERGE_RSP or INSTALL_MERGE_VIEW
        ViewId merge_id=null;        // used when type=MERGE_REQ or MERGE_RSP or INSTALL_MERGE_VIEW or CANCEL_MERGE
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

        public Address getMemeber() {
            return mbr;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer("GmsHeader");
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

        public long size() {
            long retval=Global.BYTE_SIZE *2; // type + merge_rejected

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




    public static class Request {
        static final int JOIN    = 1;
        static final int LEAVE   = 2;
        static final int SUSPECT = 3;
        static final int MERGE   = 4;
        static final int VIEW    = 5;


        int     type=-1;
        Address mbr=null;
        boolean suspected;
        Vector  coordinators=null;
        View    view=null;
        Digest  digest=null;
        List    target_members=null;

        Request(int type) {
            this.type=type;
        }

        Request(int type, Address mbr, boolean suspected, Vector coordinators) {
            this.type=type;
            this.mbr=mbr;
            this.suspected=suspected;
            this.coordinators=coordinators;
        }

        public String toString() {
            switch(type) {
                case JOIN:    return "JOIN(" + mbr + ")";
                case LEAVE:   return "LEAVE(" + mbr + ", " + suspected + ")";
                case SUSPECT: return "SUSPECT(" + mbr + ")";
                case MERGE:   return "MERGE(" + coordinators + ")";
                case VIEW:    return "VIEW (" + view.getVid() + ")";
            }
            return "<invalid (type=" + type + ")";
        }
    }




    /**
     * Class which processes JOIN, LEAVE and MERGE requests. Requests are queued and processed in FIFO order
     * @author Bela Ban
     * @version $Id: GMS.java,v 1.49 2005/12/23 14:57:06 belaban Exp $
     */
    class ViewHandler implements Runnable {
        Thread                    t;
        Queue                     q=new Queue(); // Queue<Request>
        boolean                   suspended=false;
        final static long         INTERVAL=5000;
        private static final long MAX_COMPLETION_TIME=10000;
        /** Maintains a list of the last 20 requests */
        private final BoundedList history=new BoundedList(20);

        /** Map<Object,TimeScheduler.CancellableTask>. Keeps track of Resumer tasks which have not fired yet */
        private final Map         resume_tasks=new HashMap();
        private Object            merge_id=null;


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
                if(trace)
                    log.trace("queue is closed; request " + req + " is discarded");
            }
        }


        synchronized void waitUntilCompleted(long timeout) {
            if(t != null) {
                try {
                    t.join(timeout);
                }
                catch(InterruptedException e) {
                }
            }
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
            if(trace)
                log.trace("suspended ViewHandler");
            Resumer r=new Resumer(resume_task_timeout, merge_id, resume_tasks, this);
            resume_tasks.put(merge_id, r);
            timer.add(r);
        }


        public synchronized void resume(Object merge_id) {
            if(!suspended)
                return;
            boolean same_merge_id=this.merge_id != null && merge_id != null && this.merge_id.equals(merge_id);
            same_merge_id=same_merge_id || (this.merge_id == null && merge_id == null);

            if(!same_merge_id) {
                if(warn)
                    log.warn("resume(" +merge_id+ ") does not match " + this.merge_id + ", ignoring resume()");
                return;
            }
            synchronized(resume_tasks) {
                TimeScheduler.CancellableTask task=(TimeScheduler.CancellableTask)resume_tasks.get(merge_id);
                if(task != null) {
                    task.cancel();
                    resume_tasks.remove(merge_id);
                }
            }
            resumeForce();
        }

        public synchronized void resumeForce() {
            if(q.closed())
                q.reset();
            suspended=false;
            if(trace)
                log.trace("resumed ViewHandler");
        }

        public void run() {
            Request req;
            while(!q.closed() && Thread.currentThread().equals(t)) {
                try {
                    req=(Request)q.remove(INTERVAL); // throws a TimeoutException if it runs into timeout
                    process(req);
                }
                catch(QueueClosedException e) {
                    break;
                }
                catch(TimeoutException e) {
                    break;
                }
            }
        }

        public int size() {return q.size();}
        public boolean suspended() {return suspended;}
        public String dumpQueue() {
            StringBuffer sb=new StringBuffer();
            List v=q.values();
            for(Iterator it=v.iterator(); it.hasNext();) {
                sb.append(it.next() + "\n");
            }
            return sb.toString();
        }

        public String dumpHistory() {
            StringBuffer sb=new StringBuffer();
            for(Enumeration en=history.elements(); en.hasMoreElements();) {
                sb.append(en.nextElement() + "\n");
            }
            return sb.toString();
        }

        private void process(Request req) {
            if(trace)
                log.trace("processing " + req);
            switch(req.type) {
                case Request.JOIN:
                    impl.handleJoin(req.mbr);
                    break;
                case Request.LEAVE:
                    if(req.suspected)
                        impl.suspect(req.mbr);
                    else
                        impl.handleLeave(req.mbr, req.suspected);
                    break;
                case Request.SUSPECT:
                    impl.suspect(req.mbr);
                    break;
                case Request.MERGE:
                    impl.merge(req.coordinators);
                    break;
                case Request.VIEW:
                    castViewChangeWithDest(req.view, req.digest, req.target_members);
                    break;
                default:
                    log.error("Request " + req.type + " is unknown; discarded");
            }
        }

        synchronized void start(boolean unsuspend) {
            if(q.closed())
                q.reset();
            if(unsuspend) {
                suspended=false;
                synchronized(resume_tasks) {
                    TimeScheduler.CancellableTask task=(TimeScheduler.CancellableTask)resume_tasks.get(merge_id);
                    if(task != null) {
                        task.cancel();
                        resume_tasks.remove(merge_id);
                    }
                }
            }
            merge_id=null;
            if(t == null || !t.isAlive()) {
                t=new Thread(this, "ViewHandler");
                t.setDaemon(true);
                t.start();
                if(trace)
                    log.trace("ViewHandler started");
            }
        }

        synchronized void stop(boolean flush) {
            q.close(flush);
            TimeScheduler.CancellableTask task;
            synchronized(resume_tasks) {
                for(Iterator it=resume_tasks.values().iterator(); it.hasNext();) {
                    task=(TimeScheduler.CancellableTask)it.next();
                    task.cancel();
                }
                resume_tasks.clear();
            }
            merge_id=null;
            resumeForce();
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
    static class Resumer implements TimeScheduler.CancellableTask {
        boolean           cancelled=false;
        long              interval;
        final Object      token;
        final Map         tasks;
        final ViewHandler handler;


        public Resumer(long interval, final Object token, final Map t, final ViewHandler handler) {
            this.interval=interval;
            this.token=token;
            this.tasks=t;
            this.handler=handler;
        }

        public void cancel() {
            cancelled=true;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return interval;
        }

        public void run() {
            TimeScheduler.CancellableTask t;
            boolean execute=true;
            synchronized(tasks) {
                t=(TimeScheduler.CancellableTask)tasks.get(token);
                if(t != null) {
                    t.cancel();
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
