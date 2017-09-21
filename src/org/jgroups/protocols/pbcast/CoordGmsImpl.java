
package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.*;

import java.util.*;


/**
 * Coordinator role of the Group MemberShip (GMS) protocol. Accepts JOIN and LEAVE requests and emits view changes
 * accordingly.
 * @author Bela Ban
 */
public class CoordGmsImpl extends ServerGmsImpl {
    protected static final Long  MAX_SUSPEND_TIMEOUT=30000L;

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
            if(log.isErrorEnabled()) log.error(Util.getMessage("MemberSAddressIsNull"));
            return;
        }
        if(mbr.equals(gms.local_addr))
            leaving=true;
        gms.getViewHandler().add(new Request(Request.LEAVE, mbr));

        // If we're the coord leaving, ignore gms.leave_timeout: https://issues.jboss.org/browse/JGRP-1509
        long timeout=(long)(Math.max(gms.leave_timeout, gms.view_ack_collection_timeout) * 1.10);
        gms.getViewHandler().waitUntilComplete(timeout);
    }


    public void suspect(Address mbr) {
        if(mbr.equals(gms.local_addr)) {
            if(log.isWarnEnabled()) log.warn("I am the coord and I'm suspected -- will probably leave shortly");
            return;
        }        
        Collection<Request> suspected=new LinkedHashSet<>(1);
        suspected.add(new Request(Request.SUSPECT,mbr));
        handleMembershipChange(suspected);
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
        Collection<Address> new_mbrs=new LinkedHashSet<>(requests.size());
        Collection<Address> suspected_mbrs=new LinkedHashSet<>(requests.size());
        Collection<Address> leaving_mbrs=new LinkedHashSet<>(requests.size());

        boolean self_leaving=false; // is the coord leaving

        for(Request req: requests) {
            switch(req.type) {
                case Request.JOIN:
                    new_mbrs.add(req.mbr);
                    if(req.useFlushIfPresent)
                        useFlushIfPresent=true;
                    break;
                case Request.JOIN_WITH_STATE_TRANSFER:
                    new_mbrs.add(req.mbr);
                    joinAndStateTransferInitiated=true;
                    if(req.useFlushIfPresent)
                        useFlushIfPresent=true;
                    break;
                case Request.LEAVE:
                    leaving_mbrs.add(req.mbr);
                    if(Objects.equals(gms.local_addr, req.mbr))
                        self_leaving=true;
                    break;
                case Request.SUSPECT:
                    suspected_mbrs.add(req.mbr);
                    break;
            }
        }

        new_mbrs.remove(gms.local_addr); // remove myself - cannot join myself (already joined)

        if(gms.getViewId() == null) {
            // we're probably not the coord anymore (we just left ourselves), let someone else do it
            // (client will retry when it doesn't get a response)
            log.debug("gms.view_id is null, I'm not the coordinator anymore (leaving=%b); " +
                        "the new coordinator will handle the leave request", self_leaving);
            return;
        }

        List<Address> current_members=gms.members.getMembers();
        leaving_mbrs.retainAll(current_members); // remove all elements of leaving_mbrs which are not current members
        if(suspected_mbrs.remove(gms.local_addr))
            log.warn("I am the coord and I'm being suspected -- will probably leave shortly");

        suspected_mbrs.retainAll(current_members); // remove all elements of suspected_mbrs which are not current members

        // for the members that have already joined, return the current digest and membership
        for(Iterator<Address> it=new_mbrs.iterator(); it.hasNext();) {
            Address mbr=it.next();
            if(gms.members.contains(mbr)) { // already joined: return current digest and membership
                log.trace("%s: %s already present; returning existing view %s", gms.local_addr, mbr, gms.view);
                Tuple<View,Digest> tuple=gms.getViewAndDigest();
                if(tuple != null)
                    gms.sendJoinResponse(new JoinRsp(tuple.getVal1(), tuple.getVal2()), mbr);
                else
                    log.warn("%s: did not find a digest matching view %s; dropping JOIN-RSP", gms.local_addr, gms.view);
                it.remove(); // remove it anyway, even if we didn't find a digest matching the view (joiner will retry)
            }
        }

        if(new_mbrs.isEmpty() && leaving_mbrs.isEmpty() && suspected_mbrs.isEmpty()) {
            log.trace("%s: found no members to add or remove, will not create new view", gms.local_addr);
            return;
        }
        
        View new_view=gms.getNextView(new_mbrs, leaving_mbrs, suspected_mbrs);

        if(new_view.size() == 0 && gms.local_addr != null && gms.local_addr.equals(new_view.getCreator())) {
            if(self_leaving)
                gms.initState(); // in case connect() is called again
            return;
        }

        log.trace("%s: joiners=%s, suspected=%s, leaving=%s, new view: %s",
                  gms.local_addr, new_mbrs, suspected_mbrs, leaving_mbrs, new_view);
             
        JoinRsp join_rsp=null;
        boolean hasJoiningMembers=!new_mbrs.isEmpty();
        try {            
            boolean successfulFlush =!useFlushIfPresent || !gms.flushProtocolInStack || gms.startFlush(new_view);
            if(!successfulFlush && hasJoiningMembers) {
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
                // create a new digest, which contains the new members, minus left members
                MutableDigest join_digest=new MutableDigest(new_view.getMembersRaw()).set(gms.getDigest());
                for(Address member: new_mbrs)
                    join_digest.set(member,0,0); // ... and set the new members. their first seqno will be 1

                // If the digest from NAKACK doesn't include all members of the view, we try once more; if it is still
                // incomplete, we don't send a JoinRsp back to the joiner(s). This shouldn't be a problem as they will retry
                if(join_digest.allSet() || join_digest.set(gms.getDigest()).allSet())
                    join_rsp=new JoinRsp(new_view, join_digest);
                else
                    log.warn("%s: digest does not match view (missing seqnos for %s); dropping JOIN-RSP",
                             gms.local_addr, Arrays.toString(join_digest.getNonSetMembers()));
            }

            sendLeaveResponses(leaving_mbrs); // no-op if no leaving members

            // we don't need to send the digest to existing members: https://issues.jboss.org/browse/JGRP-1317
            gms.castViewChangeAndSendJoinRsps(new_view, null, new_view.getMembers(), new_mbrs, join_rsp);
        }
        finally {
            if(hasJoiningMembers)
                gms.getDownProtocol().down(new Event(Event.RESUME_STABLE));
            if(!joinAndStateTransferInitiated && useFlushIfPresent)
                gms.stopFlush();
            if(self_leaving)
                gms.initState(); // in case connect() is called again
        }
    }


    /**
     * Called by the GMS when a VIEW is received.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to
     *                 be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        if(leaving && !new_view.containsMember(gms.local_addr))
            return;
        gms.installView(new_view, digest);
    }


    public void stop() {
        super.stop(); // sets leaving=false
        merger.stop();
    }


    
    private void sendLeaveResponses(Collection<Address> leaving_members) {
        for(Address address: leaving_members){
            Message msg=new Message(address).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.NO_RELIABILITY)
              .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.LEAVE_RSP));
            log.trace("%s: sending LEAVE response to %s", gms.local_addr, address);
            gms.getDownProtocol().down(msg);
        }
    }


}
