// $Id: ClientGmsImpl.java,v 1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.protocols.PingRsp;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;


/**
 * Client part of GMS. Whenever a new member wants to join a group, it starts in the CLIENT role.
 * No multicasts to the group will be received and processed until the member has been joined and
 * turned into a SERVER (either coordinator or participant, mostly just participant). This class
 * only implements <code>Join</code> (called by clients who want to join a certain group, and
 * <code>ViewChange</code> which is called by the coordinator that was contacted by this client, to
 * tell the client what its initial membership is.
 * @author Bela Ban
 * @version $Revision: 1.1 $
 */
public class ClientGmsImpl extends GmsImpl {
    Vector initial_mbrs=new Vector();
    Object view_installation_mutex=new Object();
    Promise join_promise=new Promise();


    public ClientGmsImpl(GMS g) {
        gms=g;
    }


    /**
     * Joins this process to a group. Determines the coordinator and sends a unicast
     * handleJoin() message to it. The coordinator returns a JoinRsp and then broadcasts the new view, which
     * contains a message digest and the current membership (including the joiner). The joiner is then
     * supposed to install the new view and the digest and starts accepting mcast messages. Previous
     * mcast messages were discarded (this is done in PBCAST).<p>
     * If successful, impl is changed to an instance of ParticipantGmsImpl.
     * Otherwise, we continue trying to send join() messages to	the coordinator,
     * until we succeed (or there is no member in the group. In this case, we create our own singleton group).
     * <p>When GMS.disable_initial_coord is set to true, then we won't become coordinator on receiving an initial
     * membership of 0, but instead will retry (forever) until we get an initial membership of > 0.
     * @param mbr Our own address (assigned through SET_LOCAL_ADDRESS)
     */
    public void join(Address mbr) {
        Address coord=null;
        JoinRsp rsp=null;
        Digest tmp_digest=null;

        join_promise.reset();
        while(true) {
            findInitialMembers();
            if(Trace.trace)
                Trace.debug("ClientGmsImpl.join()", "initial_mbrs are " + initial_mbrs);
            if(initial_mbrs.size() == 0) {
                if(gms.disable_initial_coord) {
                    if(Trace.trace)
                        Trace.info("ClientGmsImpl.join()", "received an initial membership of 0, but " +
                                                           "cannot become coordinator (disable_initial_coord=" + gms.disable_initial_coord +
                                                           "), will retry fetching the initial membership");
                    continue;
                }
                if(Trace.trace)
                    Trace.info("ClientGmsImpl.join()", "no initial members discovered: " +
                                                       "creating group as first member");
                becomeSingletonMember(mbr);
                return;
            }

            coord=determineCoord(initial_mbrs);
            if(coord == null) {
                Trace.error("ClientGmsImpl.join()", "could not determine coordinator " +
                                                    "from responses " + initial_mbrs);
                continue;
            }

            try {
                if(Trace.trace)
                    Trace.info("ClientGmsImpl.join()", "sending handleJoin(" + mbr + ") to " + coord);
                sendJoinMessage(coord, mbr);
                synchronized(join_promise) {
                    rsp=(JoinRsp)join_promise.getResult(gms.join_timeout);
                }

                if(rsp == null) {
                    if(Trace.trace)
                        Trace.warn("ClientGmsImpl.join()", "handleJoin(" + mbr + ") failed, retrying");
                }
                else {
                    // 1. Install digest
                    tmp_digest=rsp.getDigest();

                    if(tmp_digest != null) {
                        tmp_digest.incrementHighSeqno(coord); 	// see DESIGN for an explanantion
                        if(Trace.trace) Trace.info("ClientGmsImpl.join()", "digest is " + tmp_digest);
                        gms.setDigest(tmp_digest);
                    }
                    else
                        Trace.error("ClientGmsImpl.join()", "digest of JOIN response is null");

                    // 2. Install view
                    if(Trace.trace)
                        Trace.debug("ClientGmsImpl.join()", "[" + gms.local_addr +
                                                            "]: JoinRsp=" + rsp.getView() + " [size=" + rsp.getView().size() + "]\n\n");

                    if(rsp.getView() != null) {
                        if(!installView(rsp.getView())) {
                            if(Trace.trace)
                                Trace.error("ClientGmsImpl.join()", "view installation failed, " +
                                                                    "retrying to join group");
                            continue;
                        }
                        gms.passUp(new Event(Event.BECOME_SERVER));
                        gms.passDown(new Event(Event.BECOME_SERVER));
                        return;
                    }
                    else
                        Trace.error("ClientGmsImpl.join()", "view of JOIN response is null");
                }
            }
            catch(Exception e) {
                Trace.info("ClientGmsImpl.join()", "exception=" + e.toString() + ", retrying");
            }

            Util.sleep(gms.join_retry_timeout);
        }
    }


    public void leave(Address mbr) {
        wrongMethod("leave");
    }


    public void handleJoinResponse(JoinRsp join_rsp) {
        synchronized(join_promise) {
            join_promise.setResult(join_rsp); // will wake up join() method
        }
    }

    public void handleLeaveResponse() {
        wrongMethod("handleLeaveResponse");
    }


    public void suspect(Address mbr) {
        wrongMethod("suspect");
    }

    public void unsuspect(Address mbr) {
        wrongMethod("unsuspect");
    }


    public JoinRsp handleJoin(Address mbr) {
        wrongMethod("handleJoin");
        return null;
    }


    /** Returns false. Clients don't handle leave() requests */
    public void handleLeave(Address mbr, boolean suspected) {
        wrongMethod("handleLeave");
    }


    /**
     * Does nothing. Discards all views while still client.
     */
    public synchronized void handleViewChange(View new_view, Digest digest) {
        if(Trace.trace)
            Trace.debug("ClientGmsImpl.handleViewChange()", "view " + new_view.getMembers() +
                                                            " is discarded as we are not a participant");
    }


    /**
     * Called by join(). Installs the view returned by calling Coord.handleJoin() and
     * becomes coordinator.
     */
    private boolean installView(View new_view) {
        Vector mems=new_view.getMembers();
        if(Trace.trace) Trace.info("ClientGmsImpl.installView()", "new_view=" + new_view);
        if(gms.local_addr == null || mems == null || !mems.contains(gms.local_addr)) {
            Trace.error("ClientGmsImpl.installView()", "I (" + gms.local_addr +
                                                       ") am not member of " + mems + ", will not install view");
            return false;
        }
        gms.installView(new_view);
        gms.becomeParticipant();
        gms.passUp(new Event(Event.BECOME_SERVER));
        gms.passDown(new Event(Event.BECOME_SERVER));
        return true;
    }


    /** Returns immediately. Clients don't handle suspect() requests */
    public void handleSuspect(Address mbr) {
        wrongMethod("handleSuspect");
        return;
    }


    public boolean handleUpEvent(Event evt) {
        Vector tmp;

        switch(evt.getType()) {

            case Event.FIND_INITIAL_MBRS_OK:
                tmp=(Vector)evt.getArg();
                synchronized(initial_mbrs) {
                    if(tmp != null && tmp.size() > 0)
                        for(int i=0; i < tmp.size(); i++)
                            initial_mbrs.addElement(tmp.elementAt(i));
                    initial_mbrs.notify();
                }
                return false;  // don't pass up the stack
        }
        return true;
    }





    /* --------------------------- Private Methods ------------------------------------ */



    void sendJoinMessage(Address coord, Address mbr) {
        Message msg;
        GMS.GmsHeader hdr;

        msg=new Message(coord, null, null);
        hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, mbr);
        msg.putHeader(gms.getName(), hdr);
        gms.passDown(new Event(Event.MSG, msg));
    }


    /**
     * Pings initial members. Removes self before returning vector of initial members.
     * Uses IP multicast or gossiping, depending on parameters.
     */
    void findInitialMembers() {
        PingRsp ping_rsp;

        synchronized(initial_mbrs) {
            initial_mbrs.removeAllElements();
            gms.passDown(new Event(Event.FIND_INITIAL_MBRS));
            try {
                initial_mbrs.wait();
            }
            catch(Exception e) {
            }

            for(int i=0; i < initial_mbrs.size(); i++) {
                ping_rsp=(PingRsp)initial_mbrs.elementAt(i);
                if(ping_rsp.own_addr != null && gms.local_addr != null &&
                        ping_rsp.own_addr.equals(gms.local_addr)) {
                    initial_mbrs.removeElementAt(i);
                    break;
                }
            }
        }
    }


    /**
     The coordinator is determined by a majority vote. If there are an equal number of votes for
     more than 1 candidate, we determine the winner randomly.
     */
    Address determineCoord(Vector mbrs) {
        PingRsp mbr;
        Hashtable votes;
        int count, most_votes;
        Address winner=null, tmp;

        if(mbrs == null || mbrs.size() < 1)
            return null;

        votes=new Hashtable();

        // count *all* the votes (unlike the 2000 election)
        for(int i=0; i < mbrs.size(); i++) {
            mbr=(PingRsp)mbrs.elementAt(i);
            if(mbr.coord_addr != null) {
                if(!votes.containsKey(mbr.coord_addr))
                    votes.put(mbr.coord_addr, new Integer(1));
                else {
                    count=((Integer)votes.get(mbr.coord_addr)).intValue();
                    votes.put(mbr.coord_addr, new Integer(count + 1));
                }
            }
        }

        if(Trace.trace) {
            if(votes.size() > 1)
                Trace.warn("ClientGmsImpl.determineCoord()",
                           "there was more than 1 candidate for coordinator: " + votes);
            else
                Trace.info("ClientGmsImpl.determineCoord()", "election results: " + votes);
        }


        // determine who got the most votes
        most_votes=0;
        for(Enumeration e=votes.keys(); e.hasMoreElements();) {
            tmp=(Address)e.nextElement();
            count=((Integer)votes.get(tmp)).intValue();
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
        ViewId view_id=null;
        Vector mbrs=new Vector();

        // set the initial digest (since I'm the first member)
        initial_digest=new Digest(1);             // 1 member (it's only me)
        initial_digest.add(gms.local_addr, 0, 0); // initial seqno mcast by me will be 1 (highest seen +1)
        gms.setDigest(initial_digest);

        view_id=new ViewId(mbr);       // create singleton view with mbr as only member
        mbrs.addElement(mbr);
        gms.installView(new View(view_id, mbrs));
        gms.becomeCoordinator();

        gms.passUp(new Event(Event.BECOME_SERVER));
        gms.passDown(new Event(Event.BECOME_SERVER));
        if(Trace.trace)
            Trace.info("ClientGmsImpl.becomeSingletonMember()",
                       "created group (first member). My view is " + gms.view_id +
                       ", impl is " + gms.getImpl().getClass().getName());
    }


}
