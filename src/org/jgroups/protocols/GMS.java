// $Id: GMS.java,v 1.9 2004/09/15 16:21:11 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.RpcProtocol;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;

import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;


/**
 * Group membership protocol. Handles joins/leaves/crashes (suspicions) and emits new views
 * accordingly. Use VIEW_ENFORCER on top of this layer to make sure new members don't receive
 * any messages until they are members.
 * 
 * @author Bela Ban
 */
public class GMS extends RpcProtocol implements Runnable {
    private GmsImpl impl=null;
    public Address local_addr=null;
    public String group_addr=null;
    public Membership members=new Membership();
    public ViewId view_id=null;
    public long ltime=0;
    public long join_timeout=5000;
    public long join_retry_timeout=2000;
    private long flush_timeout=0;             // 0=wait forever until FLUSH completes
    private long rebroadcast_timeout=0;       // 0=wait forever until REBROADCAST completes
    private long view_change_timeout=10000;   // until all handleViewChange() RPCs have returned
    public long leave_timeout=5000;
    public Object impl_mutex=new Object();     // synchronizes event entry into impl
    public Object view_mutex=new Object();     // synchronizes view installations
    private Queue event_queue=new Queue();     // stores SUSPECT, MERGE events
    private Thread evt_thread=null;
    private Object flush_mutex=new Object();
    private FlushRsp flush_rsp=null;
    private Object rebroadcast_mutex=new Object();
    private boolean rebroadcast_unstable_msgs=true;
    private boolean print_local_addr=true;
    boolean disable_initial_coord=false; // can the member become a coord on startup or not ?
    private Hashtable impls=new Hashtable();
    final String CLIENT="Client";
    final String COORD="Coordinator";
    final String PART="Participant";


    public GMS() {
        initState();
    }


    public String getName() {
        return "GMS";
    }

    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.FLUSH));
        retval.addElement(new Integer(Event.FIND_INITIAL_MBRS));
        return retval;
    }


    public void setImpl(GmsImpl new_impl) {
        synchronized(impl_mutex) {
            impl=new_impl;
             if(log.isInfoEnabled()) log.info("changed role to " + new_impl.getClass().getName());
        }
    }


    public void start() throws Exception {
        super.start();
        if(checkForViewEnforcer(up_prot) == false) {
            if(log.isWarnEnabled()) log.warn("I need protocol layer " +
                    "VIEW_ENFORCER above me to discard messages sent to me while I'm " +
                    "not yet a group member ! Otherwise, these messages will be delivered " +
                    "to the application without checking...\n");
        }

        if(_corr != null)
            _corr.setDeadlockDetection(true);
        else
            throw new Exception("GMS.start(): cannot set deadlock detection in corr, as it is null !");
    }


    public void becomeCoordinator() {
        CoordGmsImpl tmp=(CoordGmsImpl)impls.get(COORD);

        if(tmp == null) {
            tmp=new CoordGmsImpl(this);
            tmp.leaving=false;
            tmp.received_last_view=false; // +++ ?
            impls.put(COORD, tmp);
        }

        setImpl(tmp);
    }


    public void becomeParticipant() {
        ParticipantGmsImpl tmp=(ParticipantGmsImpl)impls.get(PART);

        if(tmp == null) {
            tmp=new ParticipantGmsImpl(this);
            tmp.leaving=false;
            tmp.received_final_view=false;
            impls.put(PART, tmp);
        }
        setImpl(tmp);
    }

    public void becomeClient() {
        ClientGmsImpl tmp=(ClientGmsImpl)impls.get(CLIENT);

        if(tmp == null) {
            tmp=new ClientGmsImpl(this);
            impls.put(CLIENT, tmp);
        }
        else
            tmp.init();

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
        long vid=0;
        View v;
        Membership tmp_mbrs;
        Vector mbrs_to_remove=new Vector();

        if(old_mbrs != null && old_mbrs.size() > 0)
            for(int i=0; i < old_mbrs.size(); i++)
                mbrs_to_remove.addElement(old_mbrs.elementAt(i));
        if(suspected_mbrs != null && suspected_mbrs.size() > 0)
            for(int i=0; i < suspected_mbrs.size(); i++)
                if(!mbrs_to_remove.contains(suspected_mbrs.elementAt(i)))
                    mbrs_to_remove.addElement(suspected_mbrs.elementAt(i));

        synchronized(view_mutex) {
            vid=Math.max(view_id.getId(), ltime) + 1;
            ltime=vid;
            tmp_mbrs=members.copy();
            tmp_mbrs.merge(new_mbrs, mbrs_to_remove);
            mbrs=(Vector)tmp_mbrs.getMembers().clone();
            v=new View(local_addr, vid, mbrs);
            return v;
        }
    }


    /**
     * Return a copy of the current membership minus the suspected members: FLUSH request is not sent
     * to suspected members (because they won't respond, and not to joining members either.
     * It IS sent to leaving members (before they are allowed to leave).
     */
    Vector computeFlushDestination(Vector suspected_mbrs) {
        Vector ret=members.getMembers(); // *copy* of current membership
        if(suspected_mbrs != null && suspected_mbrs.size() > 0)
            for(int i=0; i < suspected_mbrs.size(); i++)
                ret.removeElement(suspected_mbrs.elementAt(i));
        return ret;
    }


    /**
     * Compute the destination set to which to send a VIEW_CHANGE message. This is the current
     * members + the leaving members (old_mbrs) + the joining members (new_mbrs) - the suspected
     * members.
     */
    Vector computeViewDestination(Vector new_mbrs, Vector old_mbrs, Vector suspected_mbrs) {
        Vector ret=members.getMembers(); // **copy* of current membership
        Address mbr;

        // add new members
        if(new_mbrs != null) {
            for(int i=0; i < new_mbrs.size(); i++) {
                mbr=(Address)new_mbrs.elementAt(i);
                if(!ret.contains(mbr))
                    ret.addElement(new_mbrs.elementAt(i));
            }
        }

        // old members are still in existing membership, don't need to add them explicitely


        // remove suspected members
        if(suspected_mbrs != null) {
            for(int i=0; i < suspected_mbrs.size(); i++) {
                mbr=(Address)suspected_mbrs.elementAt(i);
                ret.removeElement(suspected_mbrs.elementAt(i));
            }
        }
        return ret;
    }

    /**
     * FLUSH protocol.
     * Send to current mbrs - suspected_mbrs (not including new_mbrs, but including old_mbr)
     * Send TMP_VIEW event down,
     * this allows FLUSH/NAKACK to set membership correctly
     */

    public void flush(Vector flush_dest, Vector suspected_mbrs) {
        Vector rebroadcast_msgs=new Vector();

        if(suspected_mbrs == null)
            suspected_mbrs=new Vector();

        while(flush_dest.size() > 0) {
            flush_rsp=null;
            synchronized(flush_mutex) {
                passDown(new Event(Event.FLUSH, flush_dest));  // send FLUSH to members in flush_dest
                if(flush_rsp == null) {
                    try {
                        flush_mutex.wait(flush_timeout);
                    }
                    catch(Exception e) {
                    }
                }
            }
            if(flush_rsp == null) {
                break;
            }

            if(rebroadcast_unstable_msgs && flush_rsp.unstable_msgs != null &&
                    flush_rsp.unstable_msgs.size() > 0) {
                Message m;
                for(int i=0; i < flush_rsp.unstable_msgs.size(); i++) {
                    m=(Message)flush_rsp.unstable_msgs.elementAt(i);

                    // just add msg, NAKACK.RESEND will weed out duplicates based on
                    // <sender:id> before re-broadcasting msgs
                    rebroadcast_msgs.addElement(m);
                }
            }

            if(flush_rsp.result == true)
                break;
            else {
                if(flush_rsp.failed_mbrs != null) {
                    for(int i=0; i < flush_rsp.failed_mbrs.size(); i++) {
                        flush_dest.removeElement(flush_rsp.failed_mbrs.elementAt(i));
                        suspected_mbrs.addElement(flush_rsp.failed_mbrs.elementAt(i));
                    }
                }
            }
        } // while
         if(log.isInfoEnabled()) log.info("flushing completed.");


        // Rebroadcast unstable messages
        if(rebroadcast_unstable_msgs && rebroadcast_msgs.size() > 0) {

                if(log.isInfoEnabled()) log.info("re-broadcasting unstable messages (" +
                        rebroadcast_msgs.size() + ')');
            // NAKACK layer will rebroadcast the msgs (using the same seqnos assigned earlier)
            synchronized(rebroadcast_mutex) {
                passDown(new Event(Event.REBROADCAST_MSGS, rebroadcast_msgs));
                try {
                    rebroadcast_mutex.wait(rebroadcast_timeout);
                }
                catch(Exception e) {
                }
            }
             if(log.isInfoEnabled()) log.info("re-broadcasting messages completed");
        }
    }

    /**
     * Compute a new view, given the current view, the new members and the suspected/left
     * members.  Run view update protocol to install a new view in all members (this involves
     * casting the new view to all members). The targets for FLUSH and VIEW mcasts are
     * computed as follows:<p>
     * <pre>
     * existing          leaving        suspected          joining
     * <p/>
     * 1. FLUSH         y                 y               n                 n
     * 2. new_view      y                 n               n                 y
     * 3. tmp_view      y                 y               n                 y
     * (view_dest)
     * </pre>
     * <p/>
     * <ol>
     * <li>
     * The FLUSH is only sent to the existing and leaving members (they are the only ones that might have
     * old messages not yet seen by the group. The suspected members would not answer anyway (because they
     * have failed) and the joining members have certainly no old messages.
     * <li>
     * The new view to be installed includes the existing members plus the joining ones and
     * excludes the leaving and suspected members.
     * <li>
     * A temporary view is sent down the stack as an <em>event</em>. This allows the bottom layer
     * (e.g. UDP or TCP) to determine the members to which to send a multicast message. Compared
     * to the new view, leaving members are <em>included</em> since they have are waiting for a
     * view in which they are not members any longer before they leave. So, if we did not set a
     * temporary view, joining members would not receive the view (signalling that they have been
     * joined successfully). The temporary view is essentially the current view plus the joining
     * members (old members are still part of the current view).
     * </ol>
     */
    public void castViewChange(Vector new_mbrs, Vector old_mbrs, Vector suspected_mbrs) {
        View new_view, tmp_view;
        ViewId new_vid;
        Vector flush_dest=computeFlushDestination(suspected_mbrs);  // members to which FLUSH/VIEW is sent
        Vector view_dest=computeViewDestination(new_mbrs, old_mbrs, suspected_mbrs); // dest for view change

        // next view: current mbrs + new_mbrs - old_mbrs - suspected_mbrs
        new_view=getNextView(new_mbrs, old_mbrs, suspected_mbrs);
        new_vid=new_view.getVid();

        if(log.isInfoEnabled()) log.info("FLUSH phase, flush_dest: " + flush_dest +
                                         "\n\tview_dest: " + view_dest + "\n\tnew_view: " + new_view + '\n');
        flush(flush_dest, suspected_mbrs);
        if(log.isInfoEnabled())
            log.info("FLUSH phase done");

        /* VIEW protocol. Send to current mbrs + new_mbrs + old_mbrs - suspected_mbrs.  Since
           suspected members were removed from view_dest during the previous FLUSH round(s), we
           only need to add the new members.  Send TMP_VIEW event down, this allows
           FLUSH/NAKACK to set membership correctly */
        view_dest=computeViewDestination(new_mbrs, old_mbrs, suspected_mbrs);
        tmp_view=new View(null, view_dest);

        Event view_event=new Event(Event.TMP_VIEW, tmp_view); // so the VIEW msg is sent to the correct mbrs
        passDown(view_event); // needed e.g. by failure detector or UDP

         if(log.isInfoEnabled()) log.info("mcasting view {" + new_vid + ", " + view_dest + '}');
        passDown(new Event(Event.SWITCH_NAK_ACK));  // use ACK scheme for view bcast
        Object[] args=new Object[]{new_vid, new_view.getMembers() /* these are the mbrs in the new view */};
        MethodCall call=new MethodCall("handleViewChange", args, new String[]{ViewId.class.getName(), Vector.class.getName()});
        callRemoteMethods(view_dest, // send to all members in 'view_dest'
                call,
                GroupRequest.GET_ALL, view_change_timeout);
         if(log.isInfoEnabled()) log.info("mcasting view completed");
        passDown(new Event(Event.SWITCH_NAK));  // back to normal NAKs ...
    }


    /**
     * Assigns the new ltime. Installs view and view_id. Changes role to coordinator if necessary.
     * Sends VIEW_CHANGE event up and down the stack.
     */
    public void installView(ViewId new_view, Vector mbrs) {
        Object coord;
        int rc;

        synchronized(view_mutex) {                    // serialize access to views
            ltime=Math.max(new_view.getId(), ltime);  // compute Lamport logical time
            if(log.isInfoEnabled()) log.info("received view change, vid=" + new_view);

            /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
               This ensures that messages sent in view V1 are only received by members of V1 */
            if(checkSelfInclusion(mbrs) == false) {
                if(log.isWarnEnabled()) log.warn("I'm not member of " + mbrs + ", discarding");
                return;
            }


            if(view_id == null) {
                if(new_view == null) {
                    if(log.isErrorEnabled()) log.error("view_id and new_view are null !");
                    return;
                }
                else {  // view_id is null, new_view not: just install new_view (we're still a client)
                    view_id=(ViewId)new_view.clone();
                }
            }
            else {
                if(new_view == null) {  // this should never happen though !
                    if(log.isErrorEnabled()) log.error("new_view is null !");
                    return;
                }
                else {  // both view_id and new_view are not null
                    rc=new_view.compareTo(view_id);  // rc should always be a positive number
                    if(rc <= 0) {  // don't accept view id lower than our own
                         {
                            if(log.isWarnEnabled()) log.warn("received view <= current view; discarding it ! " +
                                    "(view_id: " + view_id + ", new_view: " + new_view + ')');
                        }
                        return;
                    }
                    else {  // the check for vid equality was okay, assign new_view to view_id

                        if(new_view.getCoordAddress() != null) {
                            view_id=new ViewId(new_view.getCoordAddress(), new_view.getId());
                        }
                        else {
                            view_id=new ViewId(view_id.getCoordAddress(), new_view.getId());
                        }
                    }
                }
            }

            if(mbrs != null && mbrs.size() > 0)
                members.set(mbrs);



            // Send VIEW_CHANGE event up and down the stack:
            Event view_event=new Event(Event.VIEW_CHANGE, makeView(members.getMembers()));
            passDown(view_event); // needed e.g. by failure detector or UDP
            passUp(view_event);

            coord=determineCoordinator();
            if(coord != null && coord.equals(local_addr)) {
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


    /**
     * Returns true if local_addr is member of mbrs, else false
     */
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




    /* ------------------------- Request handler methods ----------------------------- */

    public void join(Address mbr) {
        synchronized(impl_mutex) {
            impl.join(mbr);
        }
    }

    public void leave(Address mbr) {
        synchronized(impl_mutex) {
            impl.leave(mbr);
        }
    }

    public void suspect(Address mbr) {
        synchronized(impl_mutex) {
            impl.suspect(mbr);
        }
    }

    public void merge(Vector other_coords) {
        synchronized(impl_mutex) {
            impl.merge(other_coords);
        }
    }

    public boolean handleJoin(Address mbr) {
        synchronized(impl_mutex) {
            return impl.handleJoin(mbr);
        }
    }

    public void handleLeave(Address mbr, boolean suspected) {
        synchronized(impl_mutex) {
            impl.handleLeave(mbr, suspected);
        }
    }

    public void handleViewChange(ViewId new_view, Vector mbrs) {
//      synchronized (impl_mutex ) {
        impl.handleViewChange(new_view, mbrs);
//      }
    }

    public View handleMerge(ViewId other_vid, Vector other_members) {
        synchronized(impl_mutex) {
            if(log.isTraceEnabled())
            {
                View v=impl.handleMerge(other_vid, other_members);
                if(log.isInfoEnabled()) log.info("returning view: " + v);
                return v;
            }
            return impl.handleMerge(other_vid, other_members);
        }
    }

    public void handleSuspect(Address mbr) {
        synchronized(impl_mutex) {
            impl.handleSuspect(mbr);
        }
    }

    /* --------------------- End of Request handler methods -------------------------- */



    boolean checkForViewEnforcer(Protocol up_protocol) {
        String prot_name;

        if(up_protocol == null)
            return false;
        prot_name=up_protocol.getName();
        if(prot_name != null && "VIEW_ENFORCER".equals(prot_name))
            return true;
        return checkForViewEnforcer(up_protocol.getUpProtocol());
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p>
     * <b>Do not use <code>PassUp</code> in this method as the event is passed up
     * by default by the superclass after this method returns !</b>
     * 
     * @return boolean Defaults to true. If false, event will not be passed up the stack.
     */
    public boolean handleUpEvent(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT_OK:     // sent by someone else, but WE are responsible for sending this !
            case Event.DISCONNECT_OK:  // dito (e.g. sent by UDP layer)
                return false;


            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();

                if(print_local_addr) {
                    System.out.println("\n-------------------------------------------------------\n" +
                            "GMS: address is " + local_addr +
                            "\n-------------------------------------------------------");
                }
                return true;                         // pass up

            case Event.SUSPECT:
                try {
                    event_queue.add(evt);
                }
                catch(Exception e) {
                }
                return true;                         // pass up

            case Event.MERGE:
                try {
                    event_queue.add(evt);
                }
                catch(Exception e) {
                }
                return false;                        // don't pass up


            case Event.FLUSH_OK:
                synchronized(flush_mutex) {
                    flush_rsp=(FlushRsp)evt.getArg();
                    flush_mutex.notify();
                }
                return false;                        // don't pass up

            case Event.REBROADCAST_MSGS_OK:
                synchronized(rebroadcast_mutex) {
                    rebroadcast_mutex.notify();
                }
                return false;                        // don't pass up
        }

        return impl.handleUpEvent(evt);
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p>
     * <b>Do not use <code>PassDown</code> in this method as the event is passed down
     * by default by the superclass after this method returns !</b>
     * 
     * @return boolean Defaults to true. If false, event will not be passed down the stack.
     */
    public boolean handleDownEvent(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT:
                passDown(evt);
                try {
                    group_addr=(String)evt.getArg();
                }
                catch(ClassCastException cce) {
                    if(log.isErrorEnabled()) log.error("group address must " +
                            "be a string (group name) to make sense");
                }
                impl.join(local_addr);
                passUp(new Event(Event.CONNECT_OK));
                startEventHandlerThread();
                return false;                        // don't pass down: was already passed down

            case Event.DISCONNECT:
                impl.leave((Address)evt.getArg());
                passUp(new Event(Event.DISCONNECT_OK));
                stopEventHandlerThread();
                initState();
                return true;                         // pass down
        }

        return impl.handleDownEvent(evt);
    }


    // Priority handling, otherwise GMS.down(DISCONNECT) would block !
    // Similar to FLUSH protocol
    public void receiveDownEvent(Event evt) {
        if(evt.getType() == Event.BLOCK_OK) {
            passDown(evt);
            return;
        }
        super.receiveDownEvent(evt);
    }


    /**
     * Setup the Protocol instance acording to the configuration string
     */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("join_timeout");           // time to wait for JOIN
        if(str != null) {
            join_timeout=Long.parseLong(str);
            props.remove("join_timeout");
        }

        str=props.getProperty("print_local_addr");
        if(str != null) {
            print_local_addr=Boolean.valueOf(str).booleanValue();
            props.remove("print_local_addr");
        }

        str=props.getProperty("view_change_timeout");    // time to wait for VIEW_CHANGE
        if(str != null) {
            view_change_timeout=Long.parseLong(str);
            props.remove("view_change_timeout");
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

        str=props.getProperty("flush_timeout");           // time to wait until FLUSH completes (0=forever)
        if(str != null) {
            flush_timeout=Long.parseLong(str);
            props.remove("flush_timeout");
        }

        str=props.getProperty("rebroadcast_unstable_msgs");  // bcast unstable msgs (recvd from FLUSH)
        if(str != null) {
            rebroadcast_unstable_msgs=Boolean.valueOf(str).booleanValue();
            props.remove("rebroadcast_unstable_msgs");
        }

        str=props.getProperty("rebroadcast_timeout");     // time to wait until REBROADCAST_MSGS completes
        if(str != null) {
            rebroadcast_timeout=Long.parseLong(str);
            props.remove("rebroadcast_timeout");
        }

        str=props.getProperty("disable_initial_coord");  // allow initial mbr to become coord or not
        if(str != null) {
            disable_initial_coord=Boolean.valueOf(str).booleanValue();
            props.remove("disable_initial_coord");
        }

        if(props.size() > 0) {
            System.err.println("GMS.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    public void run() {
        Event evt;

        while(evt_thread != null && event_queue != null) {
            try {
                evt=(Event)event_queue.remove();
                switch(evt.getType()) {
                    case Event.SUSPECT:
                        impl.suspect((Address)evt.getArg());
                        break;
                    case Event.MERGE:
                        impl.merge((Vector)evt.getArg());
                        break;
                    default:
                        if(log.isErrorEnabled()) log.error("event handler thread encountered event of type " +
                                Event.type2String(evt.getType()) + ": not handled by me !");
                        break;
                }
            }
            catch(QueueClosedException closed) {
                break;
            }
            catch(Exception ex) {
                if(log.isWarnEnabled()) log.warn("exception=" + ex);
            }
        }
    }



    /* ------------------------------- Private Methods --------------------------------- */


    void initState() {
        becomeClient();
        impl.init();
        view_id=null;
        if(members != null)
            members.clear();
    }


    private void startEventHandlerThread() {
        if(event_queue == null)
            event_queue=new Queue();
        if(evt_thread == null) {
            evt_thread=new Thread(this, "GMS.EventHandlerThread");
            evt_thread.setDaemon(true);
            evt_thread.start();
        }
    }


    private void stopEventHandlerThread() {
        if(evt_thread != null) {
            event_queue.close(false);
            event_queue=null;
            evt_thread=null;
            return;
        }

        if(event_queue != null) {
            event_queue.close(false);
            event_queue=null;
        }
    }


}
