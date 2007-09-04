// $Id: GmsImpl.java,v 1.28 2007/09/04 18:39:06 vlada Exp $

package org.jgroups.protocols.pbcast;

import org.apache.commons.logging.Log;
import org.jgroups.*;
import org.jgroups.util.Digest;

import java.util.Collection;
import java.util.Vector;
import java.util.List;


public abstract class GmsImpl {
    protected GMS         gms=null;
    protected final Log   log;
    final boolean         trace;
    final boolean         warn;
    volatile boolean      leaving=false;

    protected GmsImpl() {
        log=null;
        trace=warn=false;
    }

    protected GmsImpl(GMS gms) {
        this.gms=gms;
        log=gms.getLog();
        trace=log.isTraceEnabled();
        warn=log.isWarnEnabled();
    }

    public abstract void      join(Address mbr);
    public abstract void      joinWithStateTransfer(Address local_addr);
    
    public abstract void      leave(Address mbr);

    public abstract void      handleJoinResponse(JoinRsp join_rsp);
    public abstract void      handleLeaveResponse();

    public abstract void      suspect(Address mbr);
    public abstract void      unsuspect(Address mbr);

    public void               merge(Vector<Address> other_coords)                  {} // only processed by coord
    public void               handleMergeRequest(Address sender, ViewId merge_id)  {} // only processed by coords
    public void               handleMergeResponse(MergeData data, ViewId merge_id) {} // only processed by coords
    public void               handleMergeView(MergeData data, ViewId merge_id)     {} // only processed by coords
    public void               handleMergeCancelled(ViewId merge_id)                {} // only processed by coords
    
    public abstract void      handleMembershipChange(Collection<Request> requests);
    public abstract void      handleViewChange(View new_view, Digest digest);
    public          void      handleExit() {}

    public boolean            handleUpEvent(Event evt) {return true;}

    public void               init() throws Exception {leaving=false;}
    public void               start() throws Exception {leaving=false;}
    public void               stop() {leaving=true;}



    protected void sendMergeRejectedResponse(Address sender, ViewId merge_id) {
        Message msg=new Message(sender, null, null);
        msg.setFlag(Message.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.getDownProtocol().down(new Event(Event.ENABLE_UNICASTS_TO, sender));
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }


    protected void wrongMethod(String method_name) {
        if(log.isWarnEnabled())
            log.warn(method_name + "() should not be invoked on an instance of " + getClass().getName());
    }



    /**
       Returns potential coordinator based on lexicographic ordering of member addresses. Another
       approach would be to keep track of the primary partition and return the first member if we
       are the primary partition.
     */
    protected boolean iWouldBeCoordinator(Vector new_mbrs) {
        Membership tmp_mbrs=gms.members.copy();
        tmp_mbrs.merge(new_mbrs, null);
        tmp_mbrs.sort();
        return !(tmp_mbrs.size() <= 0 || gms.local_addr == null) && gms.local_addr.equals(tmp_mbrs.elementAt(0));
    }


    public static class Request {
        static final int JOIN    = 1;
        static final int LEAVE   = 2;
        static final int SUSPECT = 3;
        static final int MERGE   = 4;
        static final int VIEW    = 5;
        static final int JOIN_WITH_STATE_TRANSFER    = 6;


        int              type=-1;
        Address          mbr;
        boolean          suspected;
        Vector<Address>  coordinators;
        View             view;
        Digest           digest;
        List<Address>    target_members;

        Request(int type) {
            this.type=type;
        }

        Request(int type, Address mbr, boolean suspected, Vector<Address> coordinators) {
            this.type=type;
            this.mbr=mbr;
            this.suspected=suspected;
            this.coordinators=coordinators;
        }

        public int getType() {
            return type;
        }

        public String toString() {
            switch(type) {
                case JOIN:    return "JOIN(" + mbr + ")";
                case JOIN_WITH_STATE_TRANSFER:    return "JOIN_WITH_STATE_TRANSFER(" + mbr + ")";
                case LEAVE:   return "LEAVE(" + mbr + ", " + suspected + ")";
                case SUSPECT: return "SUSPECT(" + mbr + ")";
                case MERGE:   return "MERGE(" + coordinators + ")";
                case VIEW:    return "VIEW (" + view.getVid() + ")";
            }
            return "<invalid (type=" + type + ")";
        }

        /**
         * Specifies whether this request can be processed with other request simultaneously
         */
        public boolean canBeProcessedTogether(Request other) {
            if(other == null)
                return false;
            int other_type=other.getType();
            return (type == JOIN || type == LEAVE || type == SUSPECT) &&
                    (other_type == JOIN || other_type == LEAVE || other_type == SUSPECT);
        }
    }

}
