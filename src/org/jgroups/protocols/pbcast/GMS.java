// $Id: GMS.java,v 1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols.pbcast;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;




/**
 * Group membership protocol. Handles joins/leaves/crashes (suspicions) and emits new views
 * accordingly. Use VIEW_ENFORCER on top of this layer to make sure new members don't receive
 * any messages until they are members.
 */
public class GMS extends Protocol {
    private GmsImpl    impl=null;
    public Properties  props=null;
    public Address     local_addr=null;
    public String      group_addr=null;
    public Membership  members=new Membership();     // real membership
    public Membership  tmp_members=new Membership(); // base for computing next view
    public Vector      joining=new Vector();         // members joined but for which no view has been yet
    public ViewId      view_id=null;
    public long        ltime=0;
    public long        join_timeout=5000;
    public long        join_retry_timeout=2000;
    public long        leave_timeout=5000;
    public long        digest_timeout=5000;        // time to wait for a digest (from PBCAST). should be fast
    public long        merge_timeout=10000;        // time to wait for all MERGE_RSPS
    public Object      impl_mutex=new Object();    // synchronizes event entry into impl
    private Object     digest_mutex=new Object();  // synchronizes the GET_DIGEST/GET_DIGEST_OK events
    private Digest     digest=null;                // holds result of GET_DIGEST event
    private Hashtable  impls=new Hashtable();
    private boolean    shun=true;
    private boolean    print_local_addr=true;
    boolean            disable_initial_coord=false; // can the member become a coord on startup or not ?
    final String       CLIENT="Client";
    final String       COORD="Coordinator";
    final String       PART="Participant";
    TimeScheduler      timer=null;


    public GMS() {
        initState();
    }


    public String getName() {
        return "GMS";
    }


    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.SET_DIGEST));
        retval.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return retval;
    }


    public void setImpl(GmsImpl new_impl) {
        synchronized(impl_mutex) {
            impl=new_impl;
            if(Trace.trace) Trace.info("GMS.setImpl()", "changed role to " + new_impl.getClass().getName());
        }
    }


    public GmsImpl getImpl() {
        return impl;
    }


    public void init() throws Exception {
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
        if(impl != null) impl.stop();
    }


    public void becomeCoordinator() {
        CoordGmsImpl tmp=(CoordGmsImpl)impls.get(COORD);

        if(tmp == null) {
            tmp=new CoordGmsImpl(this);
            impls.put(COORD, tmp);
        }
        tmp.leaving=false;
        setImpl(tmp);
        if(Trace.trace) Trace.info("GMS.becomeCoordinator()", local_addr + " became coordinator");
    }


    public void becomeParticipant() {
        ParticipantGmsImpl tmp=(ParticipantGmsImpl)impls.get(PART);

        if(tmp == null) {
            tmp=new ParticipantGmsImpl(this);
            impls.put(PART, tmp);
        }
        tmp.leaving=false;
        setImpl(tmp);
        if(Trace.trace) Trace.info("GMS.becomeParticipant()", local_addr + " became participant");
    }

    public void becomeClient() {
        ClientGmsImpl tmp=(ClientGmsImpl)impls.get(CLIENT);

        if(tmp == null) {
            tmp=new ClientGmsImpl(this);
            impls.put(CLIENT, tmp);
        }
        tmp.initial_mbrs.removeAllElements();
        setImpl(tmp);
        if(Trace.trace) Trace.info("GMS.becomeClient", local_addr + " became client");
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
        long vid=0;
        View v;
        Membership tmp_mbrs=null;
        Address tmp_mbr;

        synchronized(members) {
            if(view_id == null) {
                Trace.error("GMS.getNextView()", "view_id is null");
                return null; // this should *never* happen !
            }
            vid=Math.max(view_id.getId(), ltime) + 1;
            ltime=vid;

            if(Trace.trace)
                Trace.debug("GMS.getNextView()", "VID=" + vid + ", current members=" +
                                                 Util.printMembers(members.getMembers()) +
                                                 ", new_mbrs=" + Util.printMembers(new_mbrs) +
                                                 ", old_mbrs=" + Util.printMembers(old_mbrs) + ", suspected_mbrs=" +
                                                 Util.printMembers(suspected_mbrs));

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

            if(Trace.trace)
                Trace.debug("GMS.getNextView()", "new view is " + v);
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
    public View castViewChange(Vector new_mbrs, Vector old_mbrs, Vector suspected_mbrs) {
        View new_view;

        // next view: current mbrs + new_mbrs - old_mbrs - suspected_mbrs
        new_view=getNextView(new_mbrs, old_mbrs, suspected_mbrs);
        castViewChange(new_view);
        return new_view;
    }


    public void castViewChange(View new_view) {
        castViewChange(new_view, null);
    }


    public void castViewChange(View new_view, Digest digest) {
        Message view_change_msg;
        GmsHeader hdr;

        if(Trace.trace)
            Trace.info("GMS.castViewChange()", "mcasting view {" + new_view + "} (" + new_view.size() + " mbrs)\n");
        view_change_msg=new Message(); // bcast to all members
        hdr=new GmsHeader(GmsHeader.VIEW, new_view);
        hdr.digest=digest;
        view_change_msg.putHeader(getName(), hdr);
        passDown(new Event(Event.MSG, view_change_msg));
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

        synchronized(members) {                  // serialize access to views
            ltime=Math.max(vid.getId(), ltime);  // compute Lamport logical time

            /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
               This ensures that messages sent in view V1 are only received by members of V1 */
            if(checkSelfInclusion(mbrs) == false) {
                if(Trace.trace)
                    Trace.warn("GMS.installView()",
                               "checkSelfInclusion() failed, not a member of view " + mbrs + "; discarding view");
                if(shun) {
                    if(Trace.trace)
                        Trace.warn("GMS.installView()", "I'm being shunned, will leave and rejoin group");
                    passUp(new Event(Event.EXIT));
                }
                return;
            }



            // Discards view with id lower than our own. Will be installed without check if first view
            if(view_id != null) {
                rc=vid.compareTo(view_id);
                if(rc <= 0) {
                    Trace.error("GMS.installView()", "received view <= current view;" +
                                                     " discarding it ! (current vid: " + view_id + ", new vid: " + vid + ")");
                    return;
                }
            }

            if(Trace.trace) Trace.info("GMS.installView()", "view is " + new_view);

            // assign new_view to view_id
            view_id=vid.copy();

            // Set the membership. Take into account joining members
            if(mbrs != null && mbrs.size() > 0) {
                members.set(mbrs);
                tmp_members.set(members);
                joining.removeAll(mbrs);  // remove all members in mbrs from joining
                tmp_members.add(joining); // adjust temporary membership
            }

            // Send VIEW_CHANGE event up and down the stack:
            Event view_event=new Event(Event.VIEW_CHANGE, new_view.clone());
            passDown(view_event); // needed e.g. by failure detector or UDP
            passUp(view_event);

            coord=determineCoordinator();
            if(coord != null && coord.equals(local_addr) && !(coord.equals(vid.getCoordAddress()))) {
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
        Address new_coord=null;

        if(potential_new_coord == null) return false;

        synchronized(members) {
            if(members.size() < 2) return false;
            new_coord=(Address)members.elementAt(1);  // member at 2nd place
            if(new_coord != null && new_coord.equals(potential_new_coord))
                return true;
            return false;
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
            digest=null;
            passDown(new Event(Event.GET_DIGEST));
            try {
                digest_mutex.wait(digest_timeout);
            }
            catch(Exception ex) {
            }
            if(digest != null) {
                ret=digest;
                digest=null;
                return ret;
            }
            else {
                Trace.error("GMS.getDigest()", "digest could not be fetched from PBCAST layer");
                return null;
            }
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
                obj=msg.getHeader(getName());
                if(obj == null || !(obj instanceof GmsHeader))
                    break;
                hdr=(GmsHeader)msg.removeHeader(getName());
                switch(hdr.type) {
                    case GmsHeader.JOIN_REQ:
                        handleJoinRequest(hdr.mbr);
                        break;
                    case GmsHeader.JOIN_RSP:
                        impl.handleJoinResponse(hdr.join_rsp);
                        break;
                    case GmsHeader.LEAVE_REQ:
                        if(Trace.trace)
                            Trace.info("GMS.up()", "received LEAVE_REQ " + hdr + " from " + msg.getSrc());

                        if(hdr.mbr == null) {
                            if(Trace.trace)
                                Trace.error("GMS.up()", "LEAVE_REQ's mbr field is null");
                            return;
                        }
                        sendLeaveResponse(hdr.mbr);
                        impl.handleLeave(hdr.mbr, false);
                        break;
                    case GmsHeader.LEAVE_RSP:
                        impl.handleLeaveResponse();
                        break;
                    case GmsHeader.VIEW:
                        if(hdr.view == null) {
                            Trace.error("GMS.up()", "[VIEW]: view == null");
                            return;
                        }
                        impl.handleViewChange(hdr.view, hdr.digest);
                        break;

                    case GmsHeader.MERGE_REQ:
                        impl.handleMergeRequest(msg.getSrc(), hdr.merge_id);
                        break;

                    case GmsHeader.MERGE_RSP:
                        merge_data=new MergeData(msg.getSrc(), hdr.view, hdr.digest);
                        merge_data.merge_rejected=hdr.merge_rejected;
                        impl.handleMergeResponse(merge_data, hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_MERGE_VIEW:
                        impl.handleMergeView(new MergeData(msg.getSrc(), hdr.view, hdr.digest), hdr.merge_id);
                        break;

                    case GmsHeader.CANCEL_MERGE:
                        impl.handleMergeCancelled(hdr.merge_id);
                        break;

                    default:
                        Trace.error("GMS.up()", "GmsHeader with type=" + hdr.type + " not known");
                }
                return;  // don't pass up

            case Event.CONNECT_OK:     // sent by someone else, but WE are responsible for sending this !
            case Event.DISCONNECT_OK:  // dito (e.g. sent by UDP layer). Don't send up the stack
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
                impl.suspect((Address)evt.getArg());
                break;                               // pass up

            case Event.UNSUSPECT:
                impl.unsuspect((Address)evt.getArg());
                return;                              // discard

            case Event.MERGE:
                impl.merge((Vector)evt.getArg());
                return;                              // don't pass up
        }

        if(impl.handleUpEvent(evt))
            passUp(evt);
    }


    /**
     This method is overridden to avoid hanging on getDigest(): when a JOIN is received, the coordinator needs
     to retrieve the digest from the PBCAST layer. It therefore sends down a GET_DIGEST event, to which the PBCAST layer
     responds with a GET_DIGEST_OK event.<p>
     However, the GET_DIGEST_OK event will not be processed because the thread handling the JOIN request won't process
     the GET_DIGEST_OK event until the JOIN event returns. The receiveUpEvent() method is executed by the up-handler
     thread of the lower protocol and therefore can handle the event. All we do here is unblock the mutex on which
     JOIN is waiting, allowing JOIN to return with a valid digest. The GET_DIGEST_OK event is then discarded, because
     it won't be processed twice.
     */
    public void receiveUpEvent(Event evt) {
        if(evt.getType() == Event.GET_DIGEST_OK) {
            synchronized(digest_mutex) {
                digest=(Digest)evt.getArg();
                digest_mutex.notifyAll();
            }
            return;
        }
        super.receiveUpEvent(evt);
    }


    public void down(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT:
                passDown(evt);
                try {
                    group_addr=(String)evt.getArg();
                }
                catch(ClassCastException cce) {
                    Trace.error("GMS.down()", "[CONNECT]: group address must be a string (channel name)");
                }
                if(local_addr == null)
                    Trace.fatal("GMS.down()", "[CONNECT] local_addr is null");
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

        str=props.getProperty("shun");
        if(str != null) {
            shun=new Boolean(str).booleanValue();
            props.remove("shun");
        }

        str=props.getProperty("print_local_addr");
        if(str != null) {
            print_local_addr=new Boolean(str).booleanValue();
            props.remove("print_local_addr");
        }

        str=props.getProperty("join_timeout");           // time to wait for JOIN
        if(str != null) {
            join_timeout=new Long(str).longValue();
            props.remove("join_timeout");
        }

        str=props.getProperty("join_retry_timeout");     // time to wait between JOINs
        if(str != null) {
            join_retry_timeout=new Long(str).longValue();
            props.remove("join_retry_timeout");
        }

        str=props.getProperty("leave_timeout");           // time to wait until coord responds to LEAVE req.
        if(str != null) {
            leave_timeout=new Long(str).longValue();
            props.remove("leave_timeout");
        }

        str=props.getProperty("merge_timeout");           // time to wait for MERGE_RSPS from subgroup coordinators
        if(str != null) {
            merge_timeout=new Long(str).longValue();
            props.remove("merge_timeout");
        }

        str=props.getProperty("digest_timeout");          // time to wait for GET_DIGEST_OK from PBCAST
        if(str != null) {
            digest_timeout=new Long(str).longValue();
            props.remove("digest_timeout");
        }

        str=props.getProperty("disable_initial_coord");
        if(str != null) {
            disable_initial_coord=new Boolean(str).booleanValue();
            props.remove("disable_initial_coord");
        }

        if(props.size() > 0) {
            System.err.println("GMS.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }



    /* ------------------------------- Private Methods --------------------------------- */

    void initState() {
        becomeClient();
        view_id=null;
    }


    void handleJoinRequest(Address mbr) {
        JoinRsp join_rsp;
        Message m;
        GmsHeader hdr;

        if(mbr == null) {
            if(Trace.trace)
                Trace.error("GMS.handleJoinRequest()", "mbr is null");
            return;
        }

        if(Trace.trace)
            Trace.debug("GMS.handleJoinRequest()", "mbr=" + mbr);

        // 1. Get the new view and digest
        join_rsp=impl.handleJoin(mbr);
        if(join_rsp == null)
            Trace.error("GMS.handleJoinRequest()", impl.getClass().toString() + ".handleJoin(" + mbr +
                                                   ") returned null: will not be able to multicast new view");

        // 2. Send down a local TMP_VIEW event. This is needed by certain layers (e.g. NAKACK) to compute correct digest
        //    in case client's next request (e.g. getState()) reaches us *before* our own view change multicast.
        // Check NAKACK's TMP_VIEW handling for details
        if(join_rsp != null && join_rsp.getView() != null)
            passDown(new Event(Event.TMP_VIEW, join_rsp.getView()));

        // 3. Return result to client
        m=new Message(mbr, null, null);
        hdr=new GmsHeader(GmsHeader.JOIN_RSP, join_rsp);
        m.putHeader(getName(), hdr);
        passDown(new Event(Event.MSG, m));

        // 4. Bcast the new view
        if(join_rsp != null)
            castViewChange(join_rsp.getView());
    }


    void sendLeaveResponse(Address mbr) {
        Message msg=new Message(mbr, null, null);
        GmsHeader hdr=new GmsHeader(GmsHeader.LEAVE_RSP);
        msg.putHeader(getName(), hdr);
        passDown(new Event(Event.MSG, msg));
    }

    /* --------------------------- End of Private Methods ------------------------------- */



    public static class GmsHeader extends Header {
        public static final int JOIN_REQ=1;
        public static final int JOIN_RSP=2;
        public static final int LEAVE_REQ=3;
        public static final int LEAVE_RSP=4;
        public static final int VIEW=5;
        public static final int MERGE_REQ=6;
        public static final int MERGE_RSP=7;
        public static final int INSTALL_MERGE_VIEW=8;
        public static final int CANCEL_MERGE=9;

        int type=0;
        View view=null;            // used when type=VIEW or MERGE_RSP or INSTALL_MERGE_VIEW
        Address mbr=null;             // used when type=JOIN_REQ or LEAVE_REQ
        JoinRsp join_rsp=null;        // used when type=JOIN_RSP
        Digest digest=null;          // used when type=MERGE_RSP or INSTALL_MERGE_VIEW
        Serializable merge_id=null;        // used when type=MERGE_REQ or MERGE_RSP or INSTALL_MERGE_VIEW or CANCEL_MERGE
        boolean merge_rejected=false; // used when type=MERGE_RSP


        public GmsHeader() {
        } // used for Externalization

        public GmsHeader(int type) {
            this.type=type;
        }


        /** Used for VIEW header */
        public GmsHeader(int type, View view) {
            this.type=type;
            this.view=view;
        }


        /** Used for JOIN_REQ or LEAVE_REQ header */
        public GmsHeader(int type, Address mbr) {
            this.type=type;
            this.mbr=mbr;
        }

        /** Used for JOIN_RSP header */
        public GmsHeader(int type, JoinRsp join_rsp) {
            this.type=type;
            this.join_rsp=join_rsp;
        }


        public String toString() {
            StringBuffer sb=new StringBuffer("GmsHeader");
            sb.append("[" + type2String(type) + "]");
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
                    sb.append(": view=" + view);
                    break;

                case MERGE_REQ:
                    sb.append(": merge_id=" + merge_id);
                    break;

                case MERGE_RSP:
                    sb.append(": view=" + view + ", digest=" + digest + ", merge_rejected=" + merge_rejected +
                              ", merge_id=" + merge_id);
                    break;

                case INSTALL_MERGE_VIEW:
                    sb.append(": view=" + view + ", digest=" + digest);
                    break;

                case CANCEL_MERGE:
                    sb.append(", <merge cancelled>, merge_id=" + merge_id);
                    break;
            }
            sb.append("\n");
            return sb.toString();
        }


        public static String type2String(int type) {
            switch(type) {
                case JOIN_REQ:
                    return "JOIN_REQ";
                case JOIN_RSP:
                    return "JOIN_RSP";
                case LEAVE_REQ:
                    return "LEAVE_REQ";
                case LEAVE_RSP:
                    return "LEAVE_RSP";
                case VIEW:
                    return "VIEW";
                case MERGE_REQ:
                    return "MERGE_REQ";
                case MERGE_RSP:
                    return "MERGE_RSP";
                case INSTALL_MERGE_VIEW:
                    return "INSTALL_MERGE_VIEW";
                case CANCEL_MERGE:
                    return "CANCEL_MERGE";
                default:
                    return "<unknown>";
            }
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            out.writeObject(view);
            out.writeObject(mbr);
            out.writeObject(join_rsp);
            out.writeObject(digest);
            out.writeObject(merge_id);
            out.writeBoolean(merge_rejected);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            view=(View)in.readObject();
            mbr=(Address)in.readObject();
            join_rsp=(JoinRsp)in.readObject();
            digest=(Digest)in.readObject();
            merge_id=(Serializable)in.readObject();
            merge_rejected=in.readBoolean();
        }

    }


}
