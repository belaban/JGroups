package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Digest;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.util.*;


/**
 * Client part of GMS. Whenever a new member wants to join a group, it starts in the CLIENT role.
 * No multicasts to the group will be received and processed until the member has been joined and
 * turned into a SERVER (either coordinator or participant, mostly just participant). This class
 * only implements <code>Join</code> (called by clients who want to join a certain group, and
 * <code>ViewChange</code> which is called by the coordinator that was contacted by this client, to
 * tell the client what its initial membership is.
 * @author Bela Ban
 * @version $Revision: 1.78 $
 */
public class ClientGmsImpl extends GmsImpl {
    private final Promise<JoinRsp> join_promise=new Promise<JoinRsp>();


    public ClientGmsImpl(GMS g) {
        super(g);
    }

    public void init() throws Exception {
        super.init();
        join_promise.reset();
    }

    public void join(Address address,boolean useFlushIfPresent) {
        joinInternal(address, false,useFlushIfPresent);
    }

    public void joinWithStateTransfer(Address local_addr, boolean useFlushIfPresent) {
        joinInternal(local_addr,true,useFlushIfPresent);
    }
    
    /**
     * Joins this process to a group. Determines the coordinator and sends a
     * unicast handleJoin() message to it. The coordinator returns a JoinRsp and
     * then broadcasts the new view, which contains a message digest and the
     * current membership (including the joiner). The joiner is then supposed to
     * install the new view and the digest and starts accepting mcast messages.
     * Previous mcast messages were discarded (this is done in PBCAST).
     * <p>
     * If successful, impl is changed to an instance of ParticipantGmsImpl.
     * Otherwise, we continue trying to send join() messages to the coordinator,
     * until we succeed (or there is no member in the group. In this case, we
     * create our own singleton group).
     * <p>
     * When GMS.disable_initial_coord is set to true, then we won't become
     * coordinator on receiving an initial membership of 0, but instead will
     * retry (forever) until we get an initial membership of > 0.
     *
     * @param mbr Our own address (assigned through SET_LOCAL_ADDRESS)
     */
    private void joinInternal(Address mbr, boolean joinWithStateTransfer,boolean useFlushIfPresent) {
        Address coord=null;
        JoinRsp rsp=null;
        View tmp_view;
        long join_attempts=0;
        leaving=false;

        join_promise.reset();
        while(!leaving) {
            if(rsp == null && !join_promise.hasResult()) { // null responses means that the discovery was cancelled
                List<PingData> responses=findInitialMembers(join_promise);
                if (responses == null) {
                    // gray: we've seen this NPE here.  not sure of the cases but wanted to add more debugging info
                    throw new NullPointerException("responses returned by findInitialMembers for " + join_promise + " is null");
                }
                // Sept 2008 (bela): break if we got a belated JoinRsp (https://jira.jboss.org/jira/browse/JGRP-687)
                // Reverted above change again - bela June 2012, see https://github.com/belaban/JGroups/pull/29
                if(join_promise.hasResult()) {
                    rsp=join_promise.getResult(gms.join_timeout); // clears the result
                    if(rsp != null)
                        continue;
                }
                if(responses.isEmpty()) {
                    if(log.isTraceEnabled())
                        log.trace(gms.local_addr + ": no initial members discovered: creating group as first member");
                    becomeSingletonMember(mbr);
                    return;
                }
                if(log.isTraceEnabled())
                    log.trace(gms.local_addr + ": initial_mbrs are " + print(responses));

                coord=determineCoord(responses);
                if(coord == null) { // e.g. because we have all clients only
                    if(gms.handle_concurrent_startup == false) {
                        if(log.isTraceEnabled())
                            log.trace("handle_concurrent_startup is false; ignoring responses of initial clients");
                        becomeSingletonMember(mbr);
                        return;
                    }

                    if(log.isTraceEnabled())
                        log.trace("could not determine coordinator from responses " + responses);

                    // so the member to become singleton member (and thus coord) is the first of all clients
                    SortedSet<Address> clients=new TreeSet<Address>(); // sorted
                    clients.add(mbr); // add myself again (was removed by findInitialMembers())
                    for(PingData response: responses) {
                        Address client_addr=response.getAddress();
                        if(client_addr != null)
                            clients.add(client_addr);
                    }
                    if(log.isTraceEnabled())
                        log.trace("clients to choose new coord from are: " + clients);
                    Address new_coord=clients.first();
                    if(new_coord.equals(mbr)) {
                        if(log.isTraceEnabled())
                            log.trace("I (" + mbr + ") am the first of the clients, will become coordinator");
                        becomeSingletonMember(mbr);
                        return;
                    }
                    else {
                        if(log.isTraceEnabled())
                            log.trace("I (" + mbr
                                    + ") am not the first of the clients, waiting for another client to become coordinator");
                        Util.sleep(500);
                    }
                    continue;
                }

                if(log.isDebugEnabled())
                    log.debug("sending JOIN(" + mbr + ") to " + coord);
                sendJoinMessage(coord,mbr,joinWithStateTransfer,useFlushIfPresent);
            }

            if(rsp == null)
                rsp=join_promise.getResult(gms.join_timeout);
            if(rsp == null) {
                join_attempts++;
                if(log.isWarnEnabled())
                    log.warn("JOIN(" + mbr + ") sent to " + coord + " timed out (after " + gms.join_timeout + " ms), on try " + join_attempts);

                if(gms.max_join_attempts != 0 && join_attempts >= gms.max_join_attempts) {
                    if(log.isWarnEnabled())
                        log.warn("Too many JOIN attempts: becoming singleton");
                    becomeSingletonMember(mbr);
                    return;
                }
                continue;
            }

            // 1. check whether JOIN was rejected
            String failure=rsp.getFailReason();
            if(failure != null)
                throw new SecurityException(failure);

            // 2. Install digest
            if(rsp.getDigest() == null || rsp.getDigest().size() == 0) {
                if(log.isWarnEnabled())
                    log.warn("digest response has no senders: digest=" + rsp.getDigest());
                rsp=null;
                continue;
            }
            final Digest tmp_digest=rsp.getDigest();
            tmp_view=rsp.getView();
            if(tmp_view == null) {
                if(log.isErrorEnabled())
                    log.error("JoinRsp has a null view, skipping it");
                rsp=null;
            }
            else {
                if(!tmp_digest.contains(gms.local_addr)) {
                    throw new IllegalStateException("digest returned from " + coord + " with JOIN_RSP does not contain myself (" +
                                                      gms.local_addr + "): join response: " + rsp);
                }

                if(log.isTraceEnabled())
                    log.trace(gms.local_addr + ": JOIN-RSP=" + tmp_view + " [size=" + tmp_view.size() + "]\n\n");

                if(!installView(tmp_view, tmp_digest)) {
                    if(log.isErrorEnabled())
                        log.error("view installation failed, retrying to join group");
                    rsp=null;
                    continue;
                }

                // send VIEW_ACK to sender of view
                Message view_ack=new Message(coord, null, null);
                view_ack.setFlag(Message.OOB);
                GMS.GmsHeader tmphdr=new GMS.GmsHeader(GMS.GmsHeader.VIEW_ACK);
                view_ack.putHeader(gms.getId(), tmphdr);
                gms.getDownProtocol().down(new Event(Event.MSG, view_ack));
                return;
            }
        }
    }


    @SuppressWarnings("unchecked")
    private List<PingData> findInitialMembers(Promise<JoinRsp> promise) {
        List<PingData> responses=(List<PingData>)gms.getDownProtocol().down(new Event(Event.FIND_INITIAL_MBRS, promise));
        if(responses != null) {
            for(Iterator<PingData> iter=responses.iterator(); iter.hasNext();) {
                Address address=iter.next().getAddress();
                if(address != null && address.equals(gms.local_addr))
                    iter.remove();
            }
        }
        return responses;
    }

    public void leave(Address mbr) {
        leaving=true;
        wrongMethod("leave");
    }


    public void handleJoinResponse(JoinRsp join_rsp) {
        join_promise.setResult(join_rsp); // will wake up join() method
    }


    /* --------------------------- Private Methods ------------------------------------ */

    /**
     * Called by join(). Installs the view returned by calling Coord.handleJoin() and becomes coordinator.
     */
    private boolean installView(View new_view, Digest digest) {
        List<Address> mems=new_view.getMembers();
        if(gms.local_addr == null || mems == null || !mems.contains(gms.local_addr)) {
            if(log.isErrorEnabled())
                log.error("I (" + gms.local_addr + ") am not member of " + mems + ", will not install view");
            return false;
        }
        gms.installView(new_view, digest);
        gms.becomeParticipant();
        gms.getUpProtocol().up(new Event(Event.BECOME_SERVER));
        gms.getDownProtocol().down(new Event(Event.BECOME_SERVER));
        return true;
    }


    protected static String print(List<PingData> rsps) {
        StringBuilder sb=new StringBuilder();
        for(PingData rsp: rsps)
            sb.append(rsp.getAddress() + " ");
        return sb.toString();
    }

    void sendJoinMessage(Address coord, Address mbr,boolean joinWithTransfer, boolean useFlushIfPresent) {
        Message msg;
        GMS.GmsHeader hdr;

        msg=new Message(coord, null, null);
        msg.setFlag(Message.OOB);
        if(joinWithTransfer)
            hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER, mbr,useFlushIfPresent);
        else
            hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, mbr,useFlushIfPresent);
        msg.putHeader(gms.getId(), hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }

    /**
     The coordinator is determined by a majority vote. If there are an equal number of votes for
     more than 1 candidate, we determine the winner randomly.
     */
    private Address determineCoord(List<PingData> mbrs) {
        int count, most_votes;
        Address winner=null, tmp;

        if(mbrs == null || mbrs.size() < 1)
            return null;

        Map<Address,Integer> votes=new HashMap<Address,Integer>(5);

        // count *all* the votes (unlike the 2000 election)
        for(PingData mbr: mbrs) {
            if(mbr.hasCoord()) {
                if(!votes.containsKey(mbr.getCoordAddress()))
                    votes.put(mbr.getCoordAddress(), 1);
                else {
                    count=votes.get(mbr.getCoordAddress());
                    votes.put(mbr.getCoordAddress(), count + 1);
                }
            }
        }
        // we have seen members say someone else is coordinator but they disagree
        for(PingData mbr: mbrs) {
            // remove members who don't agree with the election (Florida)
            if (votes.containsKey(mbr.getAddress()) && (!mbr.isCoord())) {
                votes.remove(mbr.getAddress());
            }
        }

        if(votes.size() > 1) {
            if(log.isWarnEnabled()) log.warn("there was more than 1 candidate for coordinator: " + votes);
        }
        else {
            if(log.isDebugEnabled()) log.debug("election results: " + votes);
        }

        // determine who got the most votes
        most_votes=0;
        for(Map.Entry<Address,Integer> entry: votes.entrySet()) {
            tmp=entry.getKey();
            count=entry.getValue();
            if(count > most_votes) {
                winner=tmp;
                // fixed July 15 2003 (patch submitted by Darren Hobbs, patch-id=771418)
                most_votes=count;
            }
        }
        votes.clear();
        return winner;
    }



    void becomeSingletonMember(Address mbr) {
        Digest initial_digest;
        ViewId view_id;
        List<Address> mbrs=new ArrayList<Address>(1);

        // set the initial digest (since I'm the first member)
        initial_digest=new Digest(gms.local_addr, 0, 0); // initial seqno mcast by me will be 1 (highest seen +1)
        gms.setDigest(initial_digest);

        view_id=new ViewId(mbr);       // create singleton view with mbr as only member
        mbrs.add(mbr);

        View new_view=new View(view_id, mbrs);
        gms.installView(new_view);
        gms.becomeCoordinator(); // not really necessary - installView() should do it

        gms.getUpProtocol().up(new Event(Event.BECOME_SERVER));
        gms.getDownProtocol().down(new Event(Event.BECOME_SERVER));
        if(log.isDebugEnabled()) log.debug("created group (first member). My view is " + gms.getViewId() +
                                             ", impl is " + gms.getImpl().getClass().getName());
    }
}
