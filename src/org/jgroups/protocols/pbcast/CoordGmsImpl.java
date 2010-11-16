
package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.Digest;
import org.jgroups.util.MergeId;
import org.jgroups.util.MutableDigest;

import java.util.*;


/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 */
public class CoordGmsImpl extends ServerGmsImpl {
    private final Long                MAX_SUSPEND_TIMEOUT=new Long(30000);

    public CoordGmsImpl(GMS g) {
        super(g);
    }


    public MergeId getMergeId() {
        return merger.getMergeId();
    }


    public void init() throws Exception {
        super.init();
        merger.cancelMerge(null);
    }

    public void join(Address mbr,boolean useFlushIfPresent) {
        wrongMethod("join");
    }
    
    public void joinWithStateTransfer(Address mbr,boolean useFlushIfPresent) {
        wrongMethod("join");
    }

    /** The coordinator itself wants to leave the group */
    public void leave(Address mbr) {
        if(mbr == null) {
            if(log.isErrorEnabled()) log.error("member's address is null !");
            return;
        }
        if(mbr.equals(gms.local_addr))
            leaving=true;
        gms.getViewHandler().add(new Request(Request.LEAVE, mbr, false));
        gms.getViewHandler().stop(true); // wait until all requests have been processed, then close the queue and leave
        gms.getViewHandler().waitUntilCompleted(gms.leave_timeout);
    }


    public void suspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm suspected -- will probably leave shortly");
            return;
        }        
        Collection<Request> suspected=new LinkedHashSet<Request>(1);
        suspected.add(new Request(Request.SUSPECT,mbr,true));
        handleMembershipChange(suspected);
    }


    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param views A List of <em>different</em> views detected by the merge protocol
     */
    public void merge(Map<Address, View> views) {
        merger.merge(views);
    }

    public void handleMergeResponse(MergeData data, MergeId merge_id) {
        merger.handleMergeResponse(data, merge_id);
    }


    public void handleMergeCancelled(MergeId merge_id) {
        merger.handleMergeCancelled(merge_id);
    }


    /**
     * Fetches the digests from all members and installs them again. Used only for diagnosis and support; don't
     * use this otherwise !
     */
    void fixDigests() {
        merger.fixDigests();
    }



    public void handleMembershipChange(Collection<Request> requests) {
        boolean joinAndStateTransferInitiated=false;
        boolean useFlushIfPresent=gms.use_flush_if_present;
        Collection<Address> new_mbrs=new LinkedHashSet<Address>(requests.size());
        Collection<Address> suspected_mbrs=new LinkedHashSet<Address>(requests.size());
        Collection<Address> leaving_mbrs=new LinkedHashSet<Address>(requests.size());

        for(Request req: requests) {
            switch(req.type) {
                case Request.JOIN:
                    new_mbrs.add(req.mbr);
                    useFlushIfPresent=req.useFlushIfPresent;
                    break;
                case Request.JOIN_WITH_STATE_TRANSFER:
                    new_mbrs.add(req.mbr);
                    joinAndStateTransferInitiated=true;
                    useFlushIfPresent=req.useFlushIfPresent;
                    break;
                case Request.LEAVE:
                    if(req.suspected)
                        suspected_mbrs.add(req.mbr);
                    else
                        leaving_mbrs.add(req.mbr);
                    break;
                case Request.SUSPECT:
                    suspected_mbrs.add(req.mbr);
                    break;
            }
        }

        new_mbrs.remove(gms.local_addr); // remove myself - cannot join myself (already joined)        

        if(gms.view_id == null) {
            // we're probably not the coord anymore (we just left ourselves), let someone else do it
            // (client will retry when it doesn't get a response)
            if(log.isDebugEnabled())
                log.debug("gms.view_id is null, I'm not the coordinator anymore (leaving=" + String.valueOf(leaving) +
                        "); the new coordinator will handle the leave request");
            return;
        }

        Vector<Address> current_members=gms.members.getMembers();
        leaving_mbrs.retainAll(current_members); // remove all elements of leaving_mbrs which are not current members
        if(suspected_mbrs.remove(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm being suspected -- will probably leave shortly");
        }
        suspected_mbrs.retainAll(current_members); // remove all elements of suspected_mbrs which are not current members

        // for the members that have already joined, return the current digest and membership
        for(Iterator<Address> it=new_mbrs.iterator(); it.hasNext();) {
            Address mbr=it.next();
            if(gms.members.contains(mbr)) { // already joined: return current digest and membership
                if(log.isWarnEnabled())
                    log.warn(mbr + " already present; returning existing view " + gms.view);
                JoinRsp join_rsp=new JoinRsp(new View(gms.view_id, gms.members.getMembers()), gms.getDigest());
                gms.sendJoinResponse(join_rsp, mbr);
                it.remove();
            }
        }

        if(new_mbrs.isEmpty() && leaving_mbrs.isEmpty() && suspected_mbrs.isEmpty()) {
            if(log.isTraceEnabled())
                log.trace("found no members to add or remove, will not create new view");
            return;
        }
        
        View new_view=gms.getNextView(new_mbrs, leaving_mbrs, suspected_mbrs);

        if(new_view.size() == 0 && gms.local_addr != null && gms.local_addr.equals(new_view.getCreator())) {
            if(log.isTraceEnabled())
                log.trace("view " + new_view + " is empty: will not multicast it (last view)");
            if(leaving) {
                gms.initState(); // in case connect() is called again
            }
            return;
        }

        gms.up(new Event(Event.PREPARE_VIEW,new_view));
        gms.down(new Event(Event.PREPARE_VIEW,new_view));
        
        if(log.isDebugEnabled())
            log.debug("new=" + new_mbrs + ", suspected=" + suspected_mbrs + ", leaving=" + leaving_mbrs +
                    ", new view: " + new_view);
             
        JoinRsp join_rsp=null;
        boolean hasJoiningMembers=!new_mbrs.isEmpty();
        try {            
            boolean successfulFlush =!useFlushIfPresent || gms.startFlush(new_view);
            if(!successfulFlush && hasJoiningMembers){
                // Don't send a join response if the flush fails (http://jira.jboss.org/jira/browse/JGRP-759)
                // The joiner should block until the previous FLUSH completed
                sendLeaveResponses(leaving_mbrs); // we still have to send potential leave responses
                // but let the joining client timeout and send another join request
                return;
            }
            
            // we cannot garbage collect during joining a new member *if* we're the only member
            // Example: {A}, B joins, after returning JoinRsp to B, A garbage collects messages higher than those
            // in the digest returned to the client, so the client will *not* be able to ask for retransmission
            // of those messages if he misses them            
            if(hasJoiningMembers) {
                gms.getDownProtocol().down(new Event(Event.SUSPEND_STABLE, MAX_SUSPEND_TIMEOUT));
                Digest tmp=gms.getDigest(); // get existing digest
                MutableDigest join_digest=null;
                if(tmp == null){
                    log.error("received null digest from GET_DIGEST: will cause JOIN to fail");
                }
                else {
                    // create a new digest, which contains the new member
                    join_digest=new MutableDigest(tmp.size() + new_mbrs.size());
                    join_digest.add(tmp); // add the existing digest to the new one
                    for(Address member:new_mbrs)
                        join_digest.add(member, 0, 0); // ... and add the new members. their first seqno will be 1
                }
                join_rsp=new JoinRsp(new_view, join_digest != null? join_digest.copy() : null);
            }

            sendLeaveResponses(leaving_mbrs); // no-op if no leaving members                            
            gms.castViewChangeWithDest(new_view, join_rsp != null? join_rsp.getDigest() : null, join_rsp,new_mbrs);                      
        }
        finally {
            if(hasJoiningMembers)
                gms.getDownProtocol().down(new Event(Event.RESUME_STABLE));
            if(!joinAndStateTransferInitiated && useFlushIfPresent)
                gms.stopFlush();
            if(leaving) {
                gms.initState(); // in case connect() is called again
            }
        }
    }


    /**
     * Called by the GMS when a VIEW is received.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        Vector<Address> mbrs=new_view.getMembers();
        if(leaving && !mbrs.contains(gms.local_addr))
            return;
        gms.installView(new_view, digest);
    }


    public void stop() {
        super.stop(); // sets leaving=false
        merger.stop();
    }


    
    private void sendLeaveResponses(Collection<Address> leaving_members) {
        for(Address address:leaving_members){
            Message msg=new Message(address, null, null); // send an ack to the leaving member
            msg.setFlag(Message.OOB);
            GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.LEAVE_RSP);
            msg.putHeader(gms.getId(), hdr);
            gms.getDownProtocol().down(new Event(Event.MSG, msg));
        }       
    }


}
