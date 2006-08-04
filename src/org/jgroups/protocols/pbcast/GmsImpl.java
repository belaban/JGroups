// $Id: GmsImpl.java,v 1.18 2006/08/04 15:53:33 belaban Exp $

package org.jgroups.protocols.pbcast;

import org.apache.commons.logging.Log;
import org.jgroups.*;

import java.util.Vector;
import java.util.Collection;
import java.util.Set;


public abstract class GmsImpl {
    protected GMS   gms=null;
    // protected final Log   log=LogFactory.getLog(getClass());
    protected final Log   log;
    final boolean         trace;
    final boolean         warn;
    boolean               leaving=false;

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
    public abstract void      leave(Address mbr);

    public abstract void      handleJoinResponse(JoinRsp join_rsp);
    public abstract void      handleLeaveResponse();

    public abstract void      suspect(Address mbr);
    public abstract void      unsuspect(Address mbr);

    public void               merge(Vector other_coords)                           {} // only processed by coord
    public void               handleMergeRequest(Address sender, ViewId merge_id)  {} // only processed by coords
    public void               handleMergeResponse(MergeData data, ViewId merge_id) {} // only processed by coords
    public void               handleMergeView(MergeData data, ViewId merge_id)     {} // only processed by coords
    public void               handleMergeCancelled(ViewId merge_id)                {} // only processed by coords

    public abstract void      handleMembershipChange(Collection newMembers, Collection oldMembers, Collection suspectedMembers);
    public abstract void      handleViewChange(View new_view, Digest digest);
    public          void      handleExit() {}

    public boolean            handleUpEvent(Event evt) {return true;}

    public void               init() throws Exception {leaving=false;}
    public void               start() throws Exception {leaving=false;}
    public void               stop() {leaving=true;}



    protected void sendMergeRejectedResponse(Address sender, ViewId merge_id) {
        Message msg=new Message(sender, null, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.merge_rejected=true;
        hdr.merge_id=merge_id;
        msg.putHeader(gms.getName(), hdr);
        if(log.isDebugEnabled()) log.debug("response=" + hdr);
        gms.passDown(new Event(Event.MSG, msg));
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
        if(tmp_mbrs.size() <= 0 || gms.local_addr == null)
            return false;
        return gms.local_addr.equals(tmp_mbrs.elementAt(0));
    }

}
