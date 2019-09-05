
package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.util.Digest;
import org.jgroups.util.MergeId;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;


public abstract class GmsImpl {
    protected final GMS    gms;
    protected final Merger merger;
    protected final Leaver leaver;
    protected final Log    log;


    protected GmsImpl(GMS gms) {
        this.gms=gms;
        merger=gms.merger;
        leaver=gms.leaver;
        log=gms.getLog();
    }

    public abstract void   join(Address mbr, boolean useFlushIfPresent);
    public abstract void   joinWithStateTransfer(Address local_addr,boolean useFlushIfPresent);
    
    public abstract void   leave();
    public void            handleCoordLeave()        {wrongMethod("handleCoordLeave");}

    public void            handleJoinResponse(JoinRsp join_rsp) {}
    public void            handleLeaveResponse(Address sender)  {leaver.handleLeaveResponse(sender);}

    public void            suspect(Address mbr)   {}
    public void            unsuspect(Address mbr) {}

    public void            merge(Map<Address,View> views)                        {}
    public void            handleMergeRequest(Address sender, MergeId merge_id, Collection<? extends Address> mbrs)  {}
    public void            handleMergeResponse(MergeData data, MergeId merge_id) {}
    public void            handleMergeView(MergeData data, MergeId merge_id)     {}
    public void            handleMergeCancelled(MergeId merge_id)                {} // only processed by coords
    public void            handleDigestResponse(Address sender, Digest digest)   {} // only processed by coords

    public void            handleMembershipChange(Collection<Request> requests)  {}
    public void            handleViewChange(View new_view, Digest digest)        {}

    public void            init()  throws Exception {}
    public void            start() throws Exception {}
    public void            stop()                   {}



    protected void sendMergeRejectedResponse(Address sender, MergeId merge_id) {
        Message msg=new Message(sender).setFlag(Message.Flag.OOB);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getId(), hdr);
        log.debug("%s: merge response=%s", gms.local_addr, hdr);
        gms.getDownProtocol().down(msg);
    }


    protected void wrongMethod(String method_name) {
        log.warn("%s: %s() should not be invoked on an instance of %s", gms.local_addr, method_name, getClass().getName());
    }





    public static class Request {
        public static final int JOIN                     = 1;
        public static final int LEAVE                    = 2;
        public static final int COORD_LEAVE              = 3;
        public static final int SUSPECT                  = 4;
        public static final int MERGE                    = 5;
        public static final int JOIN_WITH_STATE_TRANSFER = 6;



        protected int               type=-1;
        protected Address           mbr;
        protected Map<Address,View> views; // different view on MERGE
        protected boolean           useFlushIfPresent;


        public Request(int type) {
            this.type=type;
        }

        public Request(int type, Address mbr) {
            this(type);
            this.mbr=mbr;
        }

        public Request(int type, Address mbr, Map<Address,View> views, boolean useFlushPresent) {
            this(type, mbr);
            this.views=views;
            this.useFlushIfPresent=useFlushPresent;
        }
        
        public Request(int type, Address mbr, Map<Address,View> views) {
        	this(type, mbr, views, true);
        }

        public int getType() {return type;}

        public boolean equals(Object obj) {
            Request other=(Request)obj;
            if(type != other.type)
                return false;
            switch(type) {
                case JOIN:
                case JOIN_WITH_STATE_TRANSFER:
                case LEAVE:
                case COORD_LEAVE:
                case SUSPECT:
                    return Objects.equals(mbr, other.mbr);
                case MERGE:
                    return Objects.equals(views, other.views);
                default:
                    return false;
            }
        }

        public int hashCode() {
            return type + (mbr != null? mbr.hashCode() : 0) + (views != null? views.hashCode() : 0);
        }

        public String toString() {
            switch(type) {
                case JOIN:                     return String.format("JOIN(%s)", mbr);
                case JOIN_WITH_STATE_TRANSFER: return String.format("JOIN_WITH_STATE_TRANSFER(%s)", mbr);
                case LEAVE:                    return String.format("LEAVE(%s)", mbr);
                case COORD_LEAVE:              return "COORD_LEAVE";
                case SUSPECT:                  return String.format("SUSPECT(%s)", mbr);
                case MERGE:                    return String.format("MERGE(%d views)", views.size());
                default:                       return String.format("<invalid (type=%d)", type);
            }
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
