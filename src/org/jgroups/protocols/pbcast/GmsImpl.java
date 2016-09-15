
package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.util.Digest;
import org.jgroups.util.MergeId;

import java.util.Collection;
import java.util.Map;


public abstract class GmsImpl {
    protected final GMS    gms;
    protected final Merger merger;
    protected final Log    log;
    volatile boolean       leaving=false;


    protected GmsImpl(GMS gms) {
        this.gms=gms;
        merger=gms.merger;
        log=gms.getLog();
    }

    public abstract void      join(Address mbr, boolean useFlushIfPresent);
    public abstract void      joinWithStateTransfer(Address local_addr,boolean useFlushIfPresent);
    
    public abstract void      leave(Address mbr);

    public void               handleJoinResponse(JoinRsp join_rsp) {}
    public void               handleLeaveResponse() {}

    public void               suspect(Address mbr)   {}
    public void               unsuspect(Address mbr) {}

    public void               merge(Map<Address,View> views)                        {}
    public void               handleMergeRequest(Address sender, MergeId merge_id, Collection<? extends Address> mbrs)  {}
    public void               handleMergeResponse(MergeData data, MergeId merge_id) {}
    public void               handleMergeView(MergeData data, MergeId merge_id)     {}
    public void               handleMergeCancelled(MergeId merge_id)                {} // only processed by coords
    public void               handleDigestResponse(Address sender, Digest digest)   {} // only processed by coords

    public void               handleMembershipChange(Collection<Request> requests)  {}
    public void               handleViewChange(View new_view, Digest digest)        {}

    public void               init() throws Exception {leaving=false;}
    public void               start() throws Exception {leaving=false;}
    public void               stop() {leaving=true;}



    protected void sendMergeRejectedResponse(Address sender, MergeId merge_id) {
        Message msg=new Message(sender).setFlag(Message.Flag.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getId(), hdr);
        log.debug("%s: merge response=%s", gms.local_addr, hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }


    protected void wrongMethod(String method_name) {
        log.warn("%s: %s() should not be invoked on an instance of %s", gms.local_addr, method_name, getClass().getName());
    }





    public static class Request {
        static final int JOIN    = 1;
        static final int LEAVE   = 2;
        static final int SUSPECT = 3;
        static final int MERGE   = 4;        
        static final int JOIN_WITH_STATE_TRANSFER    = 6;


        int               type=-1;
        Address           mbr;
        boolean           suspected;
        Map<Address,View> views; // different view on MERGE
        boolean           useFlushIfPresent;


        Request(int type, Address mbr, boolean suspected) {
            this.type=type;
            this.mbr=mbr;
            this.suspected=suspected;
        }

        Request(int type, Address mbr, boolean suspected, Map<Address,View> views, boolean useFlushPresent) {
            this(type, mbr, suspected);
            this.views=views;
            this.useFlushIfPresent=useFlushPresent;
        }
        
        Request(int type, Address mbr, boolean suspected, Map<Address,View> views) {
        	this(type, mbr, suspected, views, true);
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
                case MERGE:   return "MERGE(" + views.size() + " views)";               
            }
            return "<invalid (type=" + type + ")";
        }

        /**
         * Specifies whether this request can be processed with other request simultaneously
         */
        public boolean canBeProcessedTogether(Request other) {
            if(other == null)
                return false;
            int other_type=other.type;
            return (type == JOIN || type == LEAVE || type == SUSPECT) &&
                    (other_type == JOIN || other_type == LEAVE || other_type == SUSPECT);
        }
    }

}
