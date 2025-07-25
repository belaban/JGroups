package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Digest;
import org.jgroups.util.Promise;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

import java.util.*;


/**
 * Client part of GMS. Whenever a new member wants to join a group, it starts in the CLIENT role.
 * No multicasts to the group will be received and processed until the member has been joined and
 * turned into a SERVER (either coordinator or participant, mostly just participant). This class
 * only implements {@code Join} (called by clients who want to join a certain group, and
 * {@code ViewChange} which is called by the coordinator that was contacted by this client, to
 * tell the client what its initial membership is.
 * @author Bela Ban
 */
public class ClientGmsImpl extends GmsImpl {
    protected final Promise<JoinRsp> join_promise=new Promise<>();


    public ClientGmsImpl(GMS g) {
        super(g);
    }

    public void init() throws Exception {
        super.init();
        join_promise.reset();
    }

    @Override
    public void join(Address address) {
        joinInternal(address, false);
    }

    @Override
    public void joinWithStateTransfer(Address local_addr) {
        joinInternal(local_addr,true);
    }
    
    /**
     * Makes this process join a group. Determines the coordinator and sends a JOIN request to it. The coordinator
     * returns a JOIN response, then broadcasts the new view, which contains a message digest and the current membership
     * (including the joiner). The joiner then installs the new view and the digest and starts accepting messages.
     * <br/>
     * If successful, impl is changed to an instance of ParticipantGmsImpl. Otherwise, we continue trying to send JOIN
     * requests to the coordinator, until we succeed (or there is no member in the group. In this case, we
     * create our own singleton group).
     *
     * @param mbr Our own address
     */
    protected void joinInternal(Address mbr, boolean joinWithStateTransfer) {
        int  join_attempts=1;
        join_promise.reset();

        Responses responses=null; // caches responses from all discovery runs (usually there's only 1 run)
        try {
            while(!gms.isLeaving()) {
                if(installViewIfValidJoinRsp(join_promise, false))
                    return;

                long start=System.nanoTime();
                if(responses == null)
                    responses=(Responses)gms.getDownProtocol().down(new Event(Event.FIND_INITIAL_MBRS, gms.getJoinTimeout()));
                else {
                    Responses tmp=(Responses)gms.getDownProtocol().down(new Event(Event.FIND_INITIAL_MBRS, gms.getJoinTimeout()));
                    if(tmp != null) {
                        try {
                            final Responses r=responses;
                            tmp.callback(pd -> r.addResponse(pd, true));
                            responses.add(tmp, gms.getAddress());
                            tmp.waitFor(gms.join_timeout);
                        } finally {
                            tmp.done();
                        }
                    }
                }

                // Sept 2008 (bela): return if we got a belated JoinRsp (https://issues.redhat.com/browse/JGRP-687)
                if(installViewIfValidJoinRsp(join_promise, false))
                    return;

                responses.waitFor(gms.join_timeout);
                long time=System.nanoTime() - start;
                boolean empty;
                if((empty=responses.isEmpty()) || responses.isCoord(gms.getAddress())) {
                    log.info("%s: %s: creating cluster as coordinator", gms.getAddress(),
                             empty? String.format("no members discovered after %s", Util.printTime(time)) : "I'm the first member");
                    becomeSingletonMember(mbr);
                    return;
                }
                log.trace("%s: discovery took %s, members: %s", gms.getAddress(), Util.printTime(time), responses);

                List<Address> coords=getCoords(responses);

                // We didn't get any coord responses; all responses were clients. If I'm the first of the sorted clients
                // I'll become coordinator. The others will wait and then retry the discovery and join process
                if(coords == null) { // e.g. because we have all clients only
                    if(firstOfAllClients(mbr, responses))
                        return;
                }
                else {
                    if(coords.size() > 1) {
                        log.debug("%s: found multiple coords: %s", gms.getAddress(), coords);
                        Collections.shuffle(coords); // so the code below doesn't always pick the same coord
                    }
                    for(Address coord: coords) {
                        log.debug("%s: sending JOIN(%s) to %s", gms.getAddress(), mbr, coord);
                        sendJoinMessage(coord, mbr, joinWithStateTransfer);
                        if(installViewIfValidJoinRsp(join_promise, true))
                            return;
                        log.warn("%s: JOIN(%s) sent to %s timed out (after %d ms), on try %d",
                                 gms.getAddress(), mbr, coord, gms.join_timeout, join_attempts);
                    }
                }
                join_attempts++;
                if(gms.max_join_attempts > 0 && join_attempts >= gms.max_join_attempts) {
                    log.warn("%s: too many JOIN attempts (%d): becoming singleton", gms.getAddress(), join_attempts);
                    becomeSingletonMember(mbr);
                    return;
                }
            }
        }
        finally {
            if(responses != null)
                responses.done();
        }
    }


    public void leave() {
        wrongMethod("leave");
    }


    public void handleJoinResponse(JoinRsp join_rsp) {
        join_promise.setResult(join_rsp); // will wake up join() method
    }


    /* --------------------------- Private Methods ------------------------------------ */

    // Installs a new view and sends an ack if (1) the join rsp is not null, (2) it is valid and (3) the view
    // installation is successful. If true is returned, the JOIN process can be terminated, else it needs to
    // go through discovery and JOIN-REQ again in a next iteration
    protected boolean installViewIfValidJoinRsp(final Promise<JoinRsp> join_promise, boolean block_for_rsp) {
        boolean success=false;
        JoinRsp rsp=null;
        try {
            if(join_promise.hasResult())
                rsp=join_promise.getResult(1, true);
            else if(block_for_rsp)
                rsp=join_promise.getResult(gms.join_timeout, true);

            return success=rsp != null && isJoinResponseValid(rsp) && installView(rsp.getView(), rsp.getDigest());
        }
        finally {
            if(success)
                gms.sendViewAck(rsp.getView().getCreator());
        }
    }

    /** Handles the case where no coord responses were received. Returns true if we became the coord
     * (caller needs to terminate the join() call), or false when the caller needs to continue */
    protected boolean firstOfAllClients(final Address joiner, final Responses rsps) {
        log.trace("%s: could not determine coordinator from rsps %s", gms.getAddress(), rsps);

        // so the member to become singleton member (and thus coord) is the first of all clients
        SortedSet<Address> clients=new TreeSet<>();
        clients.add(joiner); // add myself again (was removed by findInitialMembers())
        for(PingData response: rsps)
            clients.add(response.getAddress());

        log.trace("%s: nodes to choose new coord from are: %s", gms.getAddress(), clients);
        Address new_coord=clients.first();
        if(new_coord.equals(joiner)) {
            log.trace("%s: I'm the FIRST of the nodes, will become coordinator", gms.getAddress());
            becomeSingletonMember(joiner);
            return true;
        }
        log.trace("%s: I'm not the first of the nodes, waiting for %d ms for another client to become coordinator",
                  gms.getAddress(), gms.all_clients_retry_timeout);
        Util.sleep(gms.all_clients_retry_timeout);
        return false;
    }

    protected boolean isJoinResponseValid(final JoinRsp rsp) {
        if(rsp.getFailReason() != null)
            throw new SecurityException(rsp.getFailReason());

        Digest tmp_digest=rsp.getDigest();
        if(tmp_digest == null || tmp_digest.capacity() == 0) {
            log.warn("%s: digest is empty: digest=%s", gms.getAddress(), rsp.getDigest());
            return false;
        }

        if(!tmp_digest.contains(gms.getAddress())) {
            log.error("%s: digest in JOIN_RSP does not contain myself; join response: %s", gms.getAddress(), rsp);
            return false;
        }

        if(rsp.getView() == null) {
            log.error("%s: JoinRsp has a null view, skipping it", gms.getAddress());
            return false;
        }
        return true;
    }


    private boolean installView(View new_view, Digest digest) {
        if(!new_view.containsMember(gms.getAddress())) {
            log.error("%s: I'm not member of %s, will not install view", gms.getAddress(), new_view);
            return false;
        }
        gms.installView(new_view, digest); // impl will be particant (or coord if singleton)
        gms.getUpProtocol().up(new Event(Event.BECOME_SERVER));
        gms.getDownProtocol().down(new Event(Event.BECOME_SERVER));
        return true;
    }


    void sendJoinMessage(Address coord, Address mbr,boolean joinWithTransfer) {
        byte type=joinWithTransfer? GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER : GMS.GmsHeader.JOIN_REQ;
        GMS.GmsHeader hdr=new GMS.GmsHeader(type, mbr);
        Message msg=new BytesMessage(coord).setFlag(Message.Flag.OOB).putHeader(gms.getId(), hdr);
        gms.getDownProtocol().down(msg);
    }


    /** Returns all members who are flagged as coordinator */
    private static List<Address> getCoords(Iterable<PingData> mbrs) {
        if(mbrs == null)
            return null;

        List<Address> coords=null;
        for(PingData mbr: mbrs) {
            if(mbr.isCoord()) {
                if(coords == null)
                    coords=new ArrayList<>();
                if(!coords.contains(mbr.getAddress()))
                    coords.add(mbr.getAddress());
            }
        }
        return coords;
    }



    void becomeSingletonMember(Address mbr) {
        View new_view=View.create(mbr, 0, mbr); // create singleton view with mbr as only member

        // set the initial digest (since I'm the first member)
        Digest initial_digest=new Digest(mbr, 0, 0);
        gms.installView(new_view, initial_digest); // impl will be coordinator
        gms.getUpProtocol().up(new Event(Event.BECOME_SERVER));
        gms.getDownProtocol().down(new Event(Event.BECOME_SERVER));
        log.debug("%s: created cluster (first member). My view is %s, impl is %s",
                  gms.getAddress(), gms.getViewId(), gms.getImpl().getClass().getSimpleName());
    }
}
