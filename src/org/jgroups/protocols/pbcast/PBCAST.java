// $Id: PBCAST.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols.pbcast;

import java.util.Vector;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Hashtable;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;


/**
 * Implementation of probabilistic broadcast. Sends group messages via unreliable multicast. Gossips regularly to
 * a random subset of group members to retransmit missing messages. Gossiping is used both for bringing all
 * members to the same state (having received the same messages) and to garbage-collect messages seen by all members
 * (gc is piggybacked in gossip messages). See DESIGN for more details.
 * @author Bela Ban
 */
public class PBCAST extends Protocol implements Runnable {
    boolean operational=false;
    long seqno=1;                  // seqno for messages. 1 for the first message
    long gossip_round=1;           // identifies the gossip (together with sender)
    Address local_addr=null;
    Hashtable digest=new Hashtable();   // stores all messages from members (key: member, val: NakReceiverWindow)
    Thread gossip_thread=null;
    GossipHandler gossip_handler=null;      // removes gossips and other requests from queue and handles them
    Queue gossip_queue=new Queue(); // (bounded) queue for incoming gossip requests
    int max_queue=100;            // max elements in gossip_queue (bounded buffer)
    long gossip_interval=5000;     // gossip every 5 seconds
    double subset=0.1;               // send gossip messages to a subset consisting of 10% of the mbrship
    long desired_avg_gossip=30000; // receive a gossip every 30 secs on average
    Vector members=new Vector();
    List gossip_list=new List();   // list of gossips received, we periodically purge it (FIFO)
    int max_gossip_cache=100;     // number of gossips to keep until gossip list is purged
    int gc_lag=30;                // how many seqnos should we lag behind (see DESIGN)
    Hashtable invalid_gossipers=new Hashtable(); // keys=Address, val=Integer (number of gossips from suspected mbrs)
    int max_invalid_gossips=2;    // max number of gossip from non-member before that member is shunned
    Vector seen_list=null;
    boolean shun=false;               // whether invalid gossipers will be shunned or not
    boolean dynamic=true;             // whether to use dynamic or static gosssip_interval (overrides gossip_interval)
    boolean skip_sleep=true;
    boolean mcast_gossip=true;        // use multicast for gossips (subset will be ignored, send to all members)


    public String getName() {
        return "PBCAST";
    }


    public Vector providedUpServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.SET_DIGEST));
        retval.addElement(new Integer(Event.GET_DIGEST_STATE));
        return retval;
    }


    public void stop() {
        stopGossipThread();
        stopGossipHandler();
        operational=false;
    }


    public void up(Event evt) {
        Message m;
        PbcastHeader hdr;
        Address sender=null;

        switch(evt.getType()) {
            case Event.MSG:
                m=(Message) evt.getArg();
                if(m.getDest() != null && !m.getDest().isMulticastAddress()) {
                    if(!(m.getHeader(getName()) instanceof PbcastHeader))
                        break; // unicast address: not null and not mcast, pass up unchanged
                }

                // discard all multicast messages until we become operational (transition from joiner to member)
                if(!operational) {
                    if(Trace.trace)
                        Trace.info("PBCAST.up()", "event was discarded as I'm not yet operational. Event: " +
                                                  Util.printEvent(evt));
                    return;  // don't pass up
                }

                if(m.getHeader(getName()) instanceof PbcastHeader)
                    hdr=(PbcastHeader) m.removeHeader(getName());
                else {
                    sender=m.getSrc();
                    if(Trace.trace)
                        Trace.error("PBCAST.up()", "PbcastHeader expected, but received header of type " +
                                                   m.getHeader(getName()).getClass().getName() + " from " + sender +
                                                   ". Passing event up unchanged");
                    break;
                }

                switch(hdr.type) {
                    case PbcastHeader.MCAST_MSG:  // messages are handled directly (high priority)
                        handleUpMessage(m, hdr);
                        return;

                        // all other requests are put in the bounded gossip queue (discarded if full). this helps to ensure
                        // that no 'gossip storms' will occur (overflowing the buffers and the network)
                    case PbcastHeader.GOSSIP:
                    case PbcastHeader.XMIT_REQ:
                    case PbcastHeader.XMIT_RSP:
                    case PbcastHeader.NOT_MEMBER:
                        try {
                            if(gossip_queue.size() >= max_queue) {
                                if(Trace.trace)
                                    Trace.warn("PBCAST.up()", "gossip request " +
                                                              PbcastHeader.type2String(hdr.type) + " discarded because " +
                                                              "gossip_queue is full (number of elements=" + gossip_queue.size() + ")");
                                return;
                            }
                            gossip_queue.add(new GossipEntry(hdr, m.getSrc(), m.getBuffer()));
                        }
                        catch(Exception ex) {
                            Trace.warn("PBCAST.up()", "exception adding request to gossip_queue, details=" + ex);
                        }
                        return;

                    default:
                        Trace.error("PBCAST.up()", "type (" + hdr.type + ") of PbcastHeader not known !");
                        return;
                }

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address) evt.getArg();
                break;  // pass up
        }

        passUp(evt);  // pass up by default
    }


    public void down(Event evt) {
        PbcastHeader hdr;
        Message m, copy;
        View v;
        Vector mbrs;
        Address key;
        NakReceiverWindow win;


        switch(evt.getType()) {

            case Event.MSG:
                m=(Message) evt.getArg();
                if(m.getDest() != null && !m.getDest().isMulticastAddress()) {
                    break; // unicast address: not null and not mcast, pass down unchanged
                }
                else {      // multicast address
                    hdr=new PbcastHeader(PbcastHeader.MCAST_MSG, seqno);
                    m.putHeader(getName(), hdr);

                    // put message in NakReceiverWindow (to be on the safe side if we don't receive it ...)
                    synchronized(digest) {
                        win=(NakReceiverWindow) digest.get(local_addr);
                        if(win == null) {
                            Trace.info("PBCAST.down()", "NakReceiverWindow for sender " + local_addr +
                                                        " not found. Creating new NakReceiverWindow starting at seqno=" + seqno);
                            win=new NakReceiverWindow(local_addr, seqno);
                            digest.put(local_addr, win);
                        }
                        copy=m.copy();
                        copy.setSrc(local_addr);
                        win.add(seqno, copy);
                    }
                    seqno++;
                    break;
                }

            case Event.SET_DIGEST:
                setDigest((Digest) evt.getArg());
                return;  // don't pass down

            case Event.GET_DIGEST:  // don't pass down
                passUp(new Event(Event.GET_DIGEST_OK, getDigest()));
                return;

            case Event.GET_DIGEST_STATE:  // don't pass down
                passUp(new Event(Event.GET_DIGEST_STATE_OK, getDigest()));
                return;

            case Event.VIEW_CHANGE:
                v=(View) evt.getArg();
                if(v == null) {
                    Trace.error("PBCAST.down(VIEW_CHANGE)", "view is null !");
                    break;
                }
                mbrs=v.getMembers();

                // update internal membership list
                synchronized(members) {
                    members.removeAllElements();
                    for(int i=0; i < mbrs.size(); i++)
                        members.addElement(mbrs.elementAt(i));
                }

                // delete all members in digest that are not in new membership list
                if(mbrs.size() > 0) {
                    synchronized(digest) {
                        for(Enumeration e=digest.keys(); e.hasMoreElements();) {
                            key=(Address) e.nextElement();
                            if(!mbrs.contains(key)) {
                                win=(NakReceiverWindow) digest.get(key);
                                win.reset();
                                digest.remove(key);
                            }
                        }
                    }
                }

                // add all members from new membership list that are not yet in digest
                for(int i=0; i < mbrs.size(); i++) {
                    key=(Address) mbrs.elementAt(i);
                    if(!digest.containsKey(key)) {
                        digest.put(key, new NakReceiverWindow(key, 1));
                    }
                }

                if(dynamic) {
                    gossip_interval=computeGossipInterval(members.size(), desired_avg_gossip);
                    if(Trace.trace)
                        Trace.info("PBCAST.down()", "VIEW_CHANGE: gossip_interval=" + gossip_interval);
                    if(gossip_thread != null) {
                        skip_sleep=true;
                        gossip_thread.interrupt(); // wake up and sleep according to the new gossip_interval
                    }
                }

                startGossipThread();  // will only be started if not yet running
                startGossipHandler();
                break;

            case Event.BECOME_SERVER:
                operational=true;
                break;
        }

        passDown(evt);
    }


    /** Gossip thread. Sends gossips containing a message digest every <code>gossip_interval</code> msecs */
    public void run() {
        while(gossip_thread != null) {  // stopGossipThread() sets gossip_thread to null
            if(dynamic) {
                gossip_interval=computeGossipInterval(members.size(), desired_avg_gossip);
                if(Trace.trace)
                    Trace.info("PBCAST.run()", "gossip_interval=" + gossip_interval);
            }

            Util.sleep(gossip_interval);
            if(skip_sleep)
                skip_sleep=false;
            else
                sendGossip();
        }
    }


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("dynamic");
        if(str != null) {
            dynamic=new Boolean(str).booleanValue();
            props.remove("dynamic");
        }

        str=props.getProperty("shun");
        if(str != null) {
            shun=new Boolean(str).booleanValue();
            props.remove("shun");
        }

        str=props.getProperty("gossip_interval");
        if(str != null) {
            gossip_interval=new Long(str).longValue();
            props.remove("gossip_interval");
        }

        str=props.getProperty("mcast_gossip");
        if(str != null) {
            mcast_gossip=new Boolean(str).booleanValue();
            props.remove("mcast_gossip");
        }

        str=props.getProperty("subset");
        if(str != null) {
            subset=new Double(str).doubleValue();
            props.remove("subset");
        }

        str=props.getProperty("desired_avg_gossip");
        if(str != null) {
            desired_avg_gossip=new Long(str).longValue();
            props.remove("desired_avg_gossip");
        }

        str=props.getProperty("max_queue");
        if(str != null) {
            max_queue=new Integer(str).intValue();
            props.remove("max_queue");
        }

        str=props.getProperty("max_gossip_cache");
        if(str != null) {
            max_gossip_cache=new Integer(str).intValue();
            props.remove("max_gossip_cache");
        }

        str=props.getProperty("gc_lag");
        if(str != null) {
            gc_lag=new Integer(str).intValue();
            props.remove("gc_lag");
        }

        if(props.size() > 0) {
            System.err.println("PBCAST.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }



    /* --------------------------------- Private Methods --------------------------------------------- */


    /**
     Ensures that FIFO is observed for all messages for a certain member. The NakReceiverWindow corresponding
     to a certain sender is looked up in a hashtable. Then, the message is added to the NakReceiverWindow.
     As many messages as possible are then removed from the table and passed up.
     */
    void handleUpMessage(Message m, PbcastHeader hdr) {
        Address sender=m.getSrc();
        NakReceiverWindow win=null;
        Message tmpmsg;
        long seqno=hdr.seqno;

        if(sender == null) {
            Trace.error("PBCAST.handleUpMessage()", "sender is null");
            return;
        }

        synchronized(digest) {
            win=(NakReceiverWindow) digest.get(sender);
            if(win == null) {
                Trace.warn("PBCAST.handleUpMessage()", "NakReceiverWindow for sender " + sender +
                                                       " not found. Creating new NakReceiverWindow starting at seqno=" + seqno);
                win=new NakReceiverWindow(sender, seqno);
                digest.put(sender, win);
            }

            // *************************************
            // The header was removed before, so we add it again for the NakReceiverWindow. When there is a
            // retransmission request, the header will already be attached to the message (both message and
            // header are *copied* into delivered_msgs when a message is removed from NakReceiverWindow).
            // *************************************
            m.putHeader(getName(), hdr);
            win.add(seqno, m);

            if(Trace.trace)
                Trace.info("PBCAST.handleUpMessage()", "receiver window for " + sender + " is " + win);

            // Try to remove as many message as possible and send them up the stack
            while((tmpmsg=win.remove()) != null) {
                tmpmsg.removeHeader(getName()); // need to remove header again, so upper protocols don't get confused
                passUp(new Event(Event.MSG, tmpmsg));
            }


            // Garbage collect messages if singleton member (because then we won't receive any gossips, triggering
            // garbage collection)
            if(members.size() == 1) {
                seqno=Math.max(seqno - gc_lag, 0);
                if(seqno <= 0) {
                }
                else {
                    if(Trace.trace)
                        Trace.info("PBCAST.handleUpMessage()", "deleting messages < " +
                                                               seqno + " from " + sender);
                    win.stable(seqno);
                }
            }
        }
    }


    /**
     * Returns for each sender the 'highest seen' seqno from the digest. Highest seen means the
     * highest seqno without any gaps, e.g. if for a sender P the messages 2 3 4 6 7 were received,
     * then only 2, 3 and 4 can be delivered, so 4 is the highest seen. 6 and 7 cannot because there
     * 5 is missing. If there are no message, the highest seen seqno is -1.
     */
    Digest getDigest() {
        Digest ret=new Digest(digest.size());
        long highest_seqno, lowest_seqno;
        Address key;
        NakReceiverWindow win;

        for(Enumeration e=digest.keys(); e.hasMoreElements();) {
            key=(Address) e.nextElement();
            win=(NakReceiverWindow) digest.get(key);
            lowest_seqno=win.getLowestSeen();
            highest_seqno=win.getHighestSeen();
            ret.add(key, lowest_seqno, highest_seqno);
        }

        Trace.info("PBCAST.getDigest()", "digest is " + ret);

        return ret;
    }


    /**
     * Sets (or resets) the contents of the 'digest' table. Its current messages will be deleted and the
     * NakReceiverTables reset.
     */
    void setDigest(Digest d) {
        NakReceiverWindow win;
        Address sender;
        long seqno=1;

        synchronized(digest) {
            for(Enumeration e=digest.elements(); e.hasMoreElements();) {
                win=(NakReceiverWindow) e.nextElement();
                win.reset();
            }
            digest.clear();
            for(int i=0; i < d.size(); i++) {
                sender=d.senderAt(i);
                seqno=d.highSeqnoAt(i);
                if(sender == null) {
                    Trace.error("PBCAST.setDigest()", "cannot set item because sender is null");
                    continue;
                }
                digest.put(sender, new NakReceiverWindow(sender, seqno + 1)); // next to expect, digest had *last* seen !
            }

        }
    }


    String printDigest() {
        long highest_seqno;
        Address key;
        NakReceiverWindow win;
        StringBuffer sb=new StringBuffer();

        for(Enumeration e=digest.keys(); e.hasMoreElements();) {
            key=(Address) e.nextElement();
            win=(NakReceiverWindow) digest.get(key);
            highest_seqno=win.getHighestSeen();
            sb.append(key + ": " + highest_seqno + "\n");
        }
        return sb.toString();
    }


    String printIncomingMessageQueue() {
        StringBuffer sb=new StringBuffer();
        NakReceiverWindow win;

        win=(NakReceiverWindow) digest.get(local_addr);
        sb.append(win);
        return sb.toString();
    }


    void startGossipThread() {
        if(gossip_thread == null) {
            gossip_thread=new Thread(this);
            gossip_thread.setDaemon(true);
            gossip_thread.start();
        }
    }


    void stopGossipThread() {
        Thread tmp;

        if(gossip_thread != null) {
            if(gossip_thread.isAlive()) {
                tmp=gossip_thread;
                gossip_thread=null;
                tmp.interrupt();
                tmp=null;
            }
        }
        gossip_thread=null;
    }


    void startGossipHandler() {
        if(gossip_handler == null) {
            gossip_handler=new GossipHandler(gossip_queue);
            gossip_handler.start();
        }
    }

    void stopGossipHandler() {
        if(gossip_handler != null) {
            gossip_handler.stop();
            gossip_handler=null;
        }
    }


    /**
     * Send a gossip message with a message digest of the highest seqnos seen per sender to a subset
     * of the current membership. Exclude self (I receive all mcasts sent by myself).
     */
    void sendGossip() {
        Vector current_mbrs=(Vector) members.clone();
        Vector subset_mbrs=null;
        Gossip gossip=null;
        Message msg;
        Address dest;
        PbcastHeader hdr;


        if(local_addr != null)
            current_mbrs.remove(local_addr); // don't pick myself

        if(mcast_gossip) {  // send gossip to all members using a multicast
            gossip=new Gossip(local_addr, gossip_round, getDigest().copy(), null); // not_seen list is null, prevents forwarding
            for(int i=0; i < current_mbrs.size(); i++)  // all members have seen this gossip. Used for garbage collection
                gossip.addToSeenList((Address) current_mbrs.elementAt(i));
            hdr=new PbcastHeader(gossip, PbcastHeader.GOSSIP);
            msg=new Message(); // null dest == multicast to all members
            msg.putHeader(getName(), hdr);

            if(Trace.trace)
                Trace.info("PBCAST.sendGossip()", "(from " + local_addr +
                           ") multicasting gossip " + gossip.shortForm() + " to all members");

            passDown(new Event(Event.MSG, msg));
        }
        else {
            subset_mbrs=Util.pickSubset(current_mbrs, subset);

            for(int i=0; i < subset_mbrs.size(); i++) {
                gossip=new Gossip(local_addr, gossip_round, getDigest().copy(), (Vector) current_mbrs.clone());
                gossip.addToSeenList(local_addr);
                hdr=new PbcastHeader(gossip, PbcastHeader.GOSSIP);
                dest=(Address) subset_mbrs.elementAt(i);
                msg=new Message(dest, null, null);
                msg.putHeader(getName(), hdr);

                if(Trace.trace)
                    Trace.info("PBCAST.sendGossip()", "(from " + local_addr +
                               ") sending gossip " + gossip.shortForm() + " to " + subset_mbrs);

                passDown(new Event(Event.MSG, msg));
            }
        }

        gossip_round++;
    }


    /**
     * MOST IMPORTANT METHOD IN THIS CLASS !! This guy really decides how a gossip reaches all members,
     * or whether it will flood the network !<p>
     * Scrutinize the gossip received and request retransmission of messages that we haven't received yet.
     * A gossip has a digest which carries for each sender the lowest and highest seqno seen. We check
     * this range against our own digest and request retransmission of missing messages if needed.<br>
     * <em>See DESIGN for a description of this method</em>
     */
    void handleGossip(Gossip gossip) {
        long my_low=0, my_high=0, their_low, their_high;
        Hashtable ht=null;
        Digest their_digest;
        Address sender=null;
        NakReceiverWindow win;
        Message msg;
        Address dest;
        Vector new_dests;
        PbcastHeader hdr;
        List missing_msgs; // list of missing messages (for retransmission) (List of Longs)


        if(Trace.trace)
            Trace.info("PBCAST.handleGossip()", "(from " + local_addr +
                                                ") received gossip " + gossip.shortForm() + " from " + gossip.sender);


        if(gossip == null || gossip.digest == null) {
            Trace.warn("PBCAST.handleGossip()", "gossip is null or digest is null");
            return;
        }


        /* 1. If gossip sender is null, we cannot ask it for missing messages anyway, so discard gossip ! */
        if(gossip.sender == null) {
            Trace.error("PBCAST.handleGossip()", "sender of gossip is null; " +
                                                 "don't know where to send XMIT_REQ to. Discarding gossip");
            return;
        }


        /* 2. Don't process the gossip if the sender of the gossip is not a member anymore. If it is a newly
           joined member, discard it as well (we can't tell the difference). When the new member will be
           added to the membership, then its gossips will be processed */
        if(!members.contains(gossip.sender)) {
            Trace.warn("PBCAST.handleGossip()", "sender " + gossip.sender +
                                                " is not a member. Gossip will not be processed");
            if(shun)
                shunInvalidGossiper(gossip.sender);
            return;
        }


        /* 3. If this gossip was received before, just discard it and return (don't process the
           same gossip twice). This prevents flooding of the gossip sender with retransmission reqs */
        while(gossip_list.size() >= max_gossip_cache) // first delete oldest gossips
            gossip_list.removeFromHead();

        if(gossip_list.contains(gossip))         // already received, don't re-broadcast
            return;
        else
            gossip_list.add(gossip.copy());      // add to list of received gossips



        /* 4. Send a HEARD_FROM event containing all members in the gossip-chain down to the FD layer.
           This ensures that we don't suspect them */
        seen_list=gossip.getSeenList();
        if(seen_list.size() > 0)
            passDown(new Event(Event.HEARD_FROM, seen_list.clone()));



        /* 5. Compare their digest against ours. Find out if some messages in the their digest are
           not in our digest. If yes, put them in the 'ht' hashtable for retransmission */
        their_digest=gossip.digest;
        for(int i=0; i < their_digest.size(); i++) {
            sender=their_digest.senderAt(i);
            their_low=their_digest.lowSeqnoAt(i);
            their_high=their_digest.highSeqnoAt(i);

            if(their_low == 0 && their_high == 0)
                continue; // won't have any messages for this sender, don't even re-send

            win=(NakReceiverWindow) digest.get(sender);
            if(win == null) {
                // this specific sender in this digest is probably not a member anymore, new digests
                // won't contain it. for now, just ignore it. if it is a new member, it will be in the next
                // gossips
                if(Trace.trace)
                    Trace.warn("PBCAST.handleGossip()", "sender " + sender + " not found, skipping...");
                continue;
            }

            my_low=win.getLowestSeen();
            my_high=win.getHighestSeen();
            if(my_high < their_high) {
                // changed by Bela (June 26 2003) - replaced my_high with my_low (not tested though !)
                if(my_low + 1 < their_low) {
                    ;
                }
                else {
                    missing_msgs=win.getMissingMessages(my_high, their_high);
                    if(missing_msgs != null) {
                        if(Trace.trace) {
                            Trace.info("PBCAST.sendXmitReq()", "asking " +
                                                               gossip.sender + " for retransmission of " +
                                                               sender + ", missing messages: " +
                                                               missing_msgs + "\nwin for " + sender + ":\n" + win + "\n");
                        }
                        if(ht == null) ht=new Hashtable();
                        ht.put(sender, missing_msgs);
                    }
                }
            }
        }



        /* 6. Send a XMIT_REQ to the sender of the gossip. The sender will then resend those messages as
           an XMIT_RSP unicast message (the messages are in its buffer, as a List) */
        if(ht == null || ht.size() == 0) {
        }
        else {
            hdr=new PbcastHeader(PbcastHeader.XMIT_REQ);
            hdr.xmit_reqs=ht;
            if(Trace.trace)
                Trace.info("PBCAST.handleGossip()", "sending XMIT_REQ to " + gossip.sender);
            msg=new Message(gossip.sender, null, null);
            msg.putHeader(getName(), hdr);
            passDown(new Event(Event.MSG, msg));
        }



        /* 7. Remove myself from 'not_seen' list. If not_seen list is empty, we can garbage-collect messages
           smaller than the digest. Since all the members have seen the gossip, it will not be re-sent */
        gossip.removeFromNotSeenList(local_addr);
        if(gossip.sizeOfNotSeenList() == 0) {
            garbageCollect(gossip.digest);
            return;
        }



        /* 8. If we make it to this point, re-send to subset of remaining members in 'not_seen' list */
        new_dests=Util.pickSubset(gossip.getNotSeenList(), subset);

        if(Trace.trace)
            Trace.info("PBCAST.handleGossip()", "(from " + local_addr +
                                                ") forwarding gossip " + gossip.shortForm() + " to " + new_dests);
        gossip.addToSeenList(local_addr);
        for(int i=0; i < new_dests.size(); i++) {
            dest=(Address) new_dests.elementAt(i);
            msg=new Message(dest, null, null);
            hdr=new PbcastHeader(gossip.copy(), PbcastHeader.GOSSIP);
            msg.putHeader(getName(), hdr);
            passDown(new Event(Event.MSG, msg));
        }
    }


    /**
     * Find the messages indicated in <code>xmit_reqs</code> and re-send them to
     * <code>requester</code>
     */
    void handleXmitRequest(Address requester, Hashtable xmit_reqs) {
        NakReceiverWindow win;
        Address sender;
        List msgs, missing_msgs, xmit_msgs;
        Message msg;

        if(requester == null) {
            Trace.error("PBCAST.handleXmitRequest()", "requester is null");
            return;
        }

        if(Trace.trace) {
            Trace.info("PBCAST.handleXmitRequest()", "retransmission requests are " + printXmitReqs(xmit_reqs));
        }
        for(Enumeration e=xmit_reqs.keys(); e.hasMoreElements();) {
            sender=(Address) e.nextElement();
            win=(NakReceiverWindow) digest.get(sender);
            if(win == null) {
                Trace.warn("PBCAST.handleXmitRequest()", "sender " + sender +
                                                         " not found in my digest; skipping retransmit request !");
                continue;
            }

            missing_msgs=(List) xmit_reqs.get(sender);
            msgs=win.getMessagesInList(missing_msgs);  // msgs to be sent back to requester



            // re-send the messages to requester. don't add a header since they already have headers
            // (when added to the NakReceiverWindow, the headers were not removed)
            xmit_msgs=new List();
            for(Enumeration en=msgs.elements(); en.hasMoreElements();) {
                msg=((Message) en.nextElement()).copy();
                xmit_msgs.add(msg);
            }

            // create a msg with the List of xmit_msgs as contents, add header
            msg=new Message(requester, null, xmit_msgs);
            msg.putHeader(getName(), new PbcastHeader(PbcastHeader.XMIT_RSP));
            passDown(new Event(Event.MSG, msg));
        }
    }


    void handleXmitRsp(List xmit_msgs) {
        Message m;
        PbcastHeader hdr;

        for(Enumeration e=xmit_msgs.elements(); e.hasMoreElements();) {
            m=(Message) e.nextElement();
            hdr=(PbcastHeader) m.removeHeader(getName());
            if(Trace.trace)
                Trace.info("PBCAST.handleXmitRsp()", "received #" + hdr.seqno + ", type=" +
                                                     PbcastHeader.type2String(hdr.type) + ", msg=" + m);
            handleUpMessage(m, hdr);
        }
    }


    String printXmitReqs(Hashtable xmit_reqs) {
        StringBuffer sb=new StringBuffer();
        Address key;
        boolean first=true;

        if(xmit_reqs == null)
            return "<null>";

        for(Enumeration e=xmit_reqs.keys(); e.hasMoreElements();) {
            key=(Address) e.nextElement();
            if(!first) {
                sb.append(", ");
            }
            else
                first=false;
            sb.append(key + ": " + xmit_reqs.get(key));
        }
        return sb.toString();
    }


    void garbageCollect(Digest gc) {
        Address sender;
        long seqno;
        NakReceiverWindow win;

        for(int i=0; i < gc.size(); i++) {
            sender=gc.senderAt(i);
            win=(NakReceiverWindow) digest.get(sender);
            if(win == null) {
                Trace.debug("PBCAST.garbageCollect()", "sender " + sender +
                                                       " not found in our message digest, skipping");
                continue;
            }
            seqno=gc.highSeqnoAt(i);
            seqno=Math.max(seqno - gc_lag, 0);
            if(seqno <= 0) {
                continue;
            }
            if(Trace.trace)
                Trace.info("PBCAST.garbageCollect()", "(from " + local_addr +
                                                      ") GC: deleting messages < " + seqno + " from " + sender);
            win.stable(seqno);
        }
    }


    /**
     * If sender of gossip is not a member, send a NOT_MEMBER to sender (after n gossips received).
     * This will cause that member to leave the group and possibly re-join.
     */
    void shunInvalidGossiper(Address invalid_gossiper) {
        int num_pings=0;
        Message shun_msg;

        if(invalid_gossipers.containsKey(invalid_gossiper)) {
            num_pings=((Integer) invalid_gossipers.get(invalid_gossiper)).intValue();
            if(num_pings >= max_invalid_gossips) {
                if(Trace.trace)
                    Trace.info("PBCAST.shunInvalidGossiper()", "sender " + invalid_gossiper +
                                                               " is not member of " + members + " ! Telling it to leave group");
                shun_msg=new Message(invalid_gossiper, null, null);
                shun_msg.putHeader(getName(), new PbcastHeader(PbcastHeader.NOT_MEMBER));
                passDown(new Event(Event.MSG, shun_msg));
                invalid_gossipers.remove(invalid_gossiper);
            }
            else {
                num_pings++;
                invalid_gossipers.put(invalid_gossiper, new Integer(num_pings));
            }
        }
        else {
            num_pings++;
            invalid_gossipers.put(invalid_gossiper, new Integer(num_pings));
        }
    }


    /** Computes the gossip_interval. See DESIGN for details */
    long computeGossipInterval(int num_mbrs, double desired_avg_gossip) {
        return getRandom((long) (num_mbrs * desired_avg_gossip * 2));
    }


    long getRandom(long range) {
        return (long) ((Math.random() * range) % range);
    }


    /* ------------------------------- End of Private Methods ---------------------------------------- */


    private static class GossipEntry {
        PbcastHeader hdr=null;
        Address sender=null;
        byte[] data=null;

        GossipEntry(PbcastHeader hdr, Address sender, byte[] data) {
            this.hdr=hdr;
            this.sender=sender;
            this.data=data;
        }

        public String toString() {
            return "hdr=" + hdr + ", sender=" + sender + ", data=" + data;
        }
    }


    /**
     Handles gossip and retransmission requests. Removes requests from a (bounded) queue.
     */
    private class GossipHandler implements Runnable {
        Thread t=null;
        Queue gossip_queue;


        GossipHandler(Queue q) {
            gossip_queue=q;
        }


        void start() {
            if(t == null) {
                t=new Thread(this, "PBCAST.GossipHandlerThread");
                t.setDaemon(true);
                t.start();
            }
        }


        void stop() {
            Thread tmp;
            if(t != null && t.isAlive()) {
                tmp=t;
                t=null;
                if(gossip_queue != null)
                    gossip_queue.close(false); // don't flush elements
                tmp.interrupt();
            }
            t=null;
        }


        public void run() {
            GossipEntry entry;
            PbcastHeader hdr;
            List xmit_msgs;
            byte[] data;

            while(t != null && gossip_queue != null) {
                try {
                    entry=(GossipEntry) gossip_queue.remove();
                    hdr=entry.hdr;
                    if(hdr == null) {
                        Trace.error("PBCAST.GossipHandler.run()", "gossip entry has no PbcastHeader");
                        continue;
                    }

                    switch(hdr.type) {

                        case PbcastHeader.GOSSIP:
                            handleGossip(hdr.gossip);
                            break;

                        case PbcastHeader.XMIT_REQ:
                            if(hdr.xmit_reqs == null) {
                                Trace.warn("PBCAST.GossipHandler.run(MSG.XMIT_REQ)", "request is null !");
                                break;
                            }
                            handleXmitRequest(entry.sender, hdr.xmit_reqs);
                            break;

                        case PbcastHeader.XMIT_RSP:
                            data=entry.data;
                            if(data == null) {
                                Trace.warn("PBCAST.GossipHandler.run(XMIT_RSP)", "buffer is null (no xmitted msgs)");
                                break;
                            }
                            try {
                                xmit_msgs=(List) Util.objectFromByteBuffer(data);
                            }
                            catch(Exception ex) {
                                Trace.error("PBCAST.GossipHandler.run(XMIT_RSP)", ex.getMessage());
                                break;
                            }
                            handleXmitRsp(xmit_msgs);
                            break;

                        case PbcastHeader.NOT_MEMBER:  // we are shunned
                            if(shun) {
                                Trace.info("PBCAST.GossipHandler.run(NOT_MEMBER)",
                                           "I am being shunned. Will leave and re-join");
                                passUp(new Event(Event.EXIT));
                            }
                            break;

                        default:
                            Trace.error("PBCAST.GossipHandler.run(MSG)", "type (" + hdr.type +
                                                                         ") of PbcastHeader not known !");
                            return;
                    }
                }
                catch(QueueClosedException closed) {
                    break;
                }
            }
        }
    }

}

