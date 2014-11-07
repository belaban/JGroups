package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Digest;
import org.jgroups.util.Promise;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;


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
    protected final Promise<JoinRsp> join_promise=new Promise<JoinRsp>();


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
    protected void joinInternal(Address mbr, boolean joinWithStateTransfer, boolean useFlushIfPresent) {
        long          join_attempts=0;
        leaving=false;
        join_promise.reset();
        while(!leaving) {
            if(installViewIfValidJoinRsp(join_promise, false))
                return;

            long start=System.currentTimeMillis();
            Responses responses=(Responses)gms.getDownProtocol().down(Event.FIND_INITIAL_MBRS_EVT);

            // Sept 2008 (bela): break if we got a belated JoinRsp (https://jira.jboss.org/jira/browse/JGRP-687)
            if(installViewIfValidJoinRsp(join_promise, false))
                return;

            responses.waitFor(gms.join_timeout);
            responses.done();
            long diff=System.currentTimeMillis() - start;
            if(responses.isEmpty()) {
                log.trace("%s: no members discovered after %d ms: creating cluster as first member", gms.local_addr, diff);
                becomeSingletonMember(mbr);
                return;
            }
            log.trace("%s: discovery took %d ms, members: %s", gms.local_addr, diff, responses);

            List<Address> coords=getCoords(responses);

            // We didn't get any coord responses; all responses were clients. If I'm the first of the sorted clients
            // I'll become coordinator. The others will wait and then retry the discovery and join process
            if(coords == null) { // e.g. because we have all clients only
                if(firstOfAllClients(mbr, responses))
                    return;
                continue;
            }

            if(coords.size() > 1)
                log.debug("%s: found multiple coords: %s", gms.local_addr, coords);

            join_attempts++;
            for(Address coord: coords) {
                log.debug("%s: sending JOIN(%s) to %s", gms.local_addr, mbr, coord);
                sendJoinMessage(coord, mbr, joinWithStateTransfer, useFlushIfPresent);
                if(installViewIfValidJoinRsp(join_promise, true))
                    return;
                log.warn("%s: JOIN(%s) sent to %s timed out (after %d ms), on try %d",
                         gms.local_addr, mbr, coord, gms.join_timeout, join_attempts);
            }

            if(gms.max_join_attempts != 0 && join_attempts >= gms.max_join_attempts) {
                log.warn("%s: too many JOIN attempts (%d): becoming singleton", gms.local_addr, join_attempts);
                becomeSingletonMember(mbr);
                return;
            }
        }
    }




    public void leave(Address mbr) {
        leaving=true;
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
                sendViewAck(rsp.getView().getCreator());
        }
    }

    /** Handles the case where no coord responses were received. Returns true if we became the coord
     * (caller needs to terminate the join() call), or false when the caller needs to continue */
    protected boolean firstOfAllClients(final Address joiner, final Responses rsps) {
        log.trace("%s: could not determine coordinator from rsps %s", gms.local_addr, rsps);

        // so the member to become singleton member (and thus coord) is the first of all clients
        SortedSet<Address> clients=new TreeSet<Address>();
        clients.add(joiner); // add myself again (was removed by findInitialMembers())
        for(PingData response: rsps)
            clients.add(response.getAddress());

        log.trace("%s: nodes to choose new coord from are: %s", gms.local_addr, clients);
        Address new_coord=clients.first();
        if(new_coord.equals(joiner)) {
            log.trace("%s: I (%s) am the first of the nodes, will become coordinator", gms.local_addr, joiner);
            becomeSingletonMember(joiner);
            return true;
        }
        log.trace("%s: I (%s) am not the first of the nodes, waiting for another client to become coordinator",
                  gms.local_addr, joiner);
        Util.sleep(500);
        return false;
    }

    protected boolean isJoinResponseValid(final JoinRsp rsp) {
        if(rsp.getFailReason() != null)
            throw new SecurityException(rsp.getFailReason());

        Digest tmp_digest=rsp.getDigest();
        if(tmp_digest == null || tmp_digest.capacity() == 0) {
            log.warn("%s: digest is empty: digest=%s", gms.local_addr, rsp.getDigest());
            return false;
        }

        if(!tmp_digest.contains(gms.local_addr)) {
            log.error("%s: digest in JOIN_RSP does not contain myself; join response: %s", gms.local_addr, rsp);
            return false;
        }

        if(rsp.getView() == null) {
            log.error("%s: JoinRsp has a null view, skipping it", gms.local_addr);
            return false;
        }
        return true;
    }



    /**
     * Called by join(). Installs the view returned by calling Coord.handleJoin() and becomes coordinator.
     */
    private boolean installView(View new_view, Digest digest) {
        if(!new_view.containsMember(gms.local_addr)) {
            log.error("%s: I'm not member of %s, will not install view", gms.local_addr, new_view);
            return false;
        }
        gms.installView(new_view, digest);
        if(gms.impl == null || gms.impl instanceof ClientGmsImpl) // installView() should have set the role (impl)
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

    protected static String print(Responses rsps) {
        StringBuilder sb=new StringBuilder();
        for(PingData rsp: rsps)
            sb.append(rsp.getAddress() + " ");
        return sb.toString();
    }

    void sendJoinMessage(Address coord, Address mbr,boolean joinWithTransfer, boolean useFlushIfPresent) {
        Message msg=new Message(coord).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL);
        GMS.GmsHeader hdr=joinWithTransfer? new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER, mbr,useFlushIfPresent)
          : new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, mbr,useFlushIfPresent);
        msg.putHeader(gms.getId(), hdr);
        gms.getDownProtocol().down(new Event(Event.MSG, msg));
    }

    void sendViewAck(Address coord) {
        Message view_ack=new Message(coord).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.VIEW_ACK));
        gms.getDownProtocol().down(new Event(Event.MSG, view_ack)); // send VIEW_ACK to sender of view
    }

    /** Returns all members whose PingData is flagged as coordinator */
    private static List<Address> getCoords(Iterable<PingData> mbrs) {
        if(mbrs == null)
            return null;

        List<Address> coords=null;
        for(PingData mbr: mbrs) {
            if(mbr.isCoord()) {
                if(coords == null)
                    coords=new ArrayList<Address>();
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
        gms.installView(new_view, initial_digest);
        gms.becomeCoordinator(); // not really necessary - installView() should do it

        gms.getUpProtocol().up(new Event(Event.BECOME_SERVER));
        gms.getDownProtocol().down(new Event(Event.BECOME_SERVER));
        log.debug("%s: created cluster (first member). My view is %s, impl is %s",
                  gms.getLocalAddress(), gms.getViewId(), gms.getImpl().getClass().getName());
    }
}
