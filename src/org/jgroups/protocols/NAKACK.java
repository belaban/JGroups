// $Id: NAKACK.java,v 1.4 2004/03/30 06:47:21 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.util.List;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;


/**
 * Negative AcKnowledgement layer (NAKs), paired with positive ACKs. The default is to send a message
 * using NAKs: the sender sends messages with monotonically increasing seqnos, receiver requests
 * retransmissions of missing messages (gaps). When a SWITCH_NAK_ACK event is received, the mode
 * is switched to using NAK_ACKS: the sender still uses monotonically increasing seqnos, but the receiver
 * acknowledges every message. NAK and NAK_ACK seqnos are the same, when switching the mode, the current
 * seqno is reused. Both NAK and NAK_ACK messages use the current view ID in which the message is sent to
 * queue messages destined for an upcoming view, or discard messages sent in a previous view. Both modes
 * reset their seqnos to 0 when receiving a view change. The NAK_ACK scheme is used for broadcasting
 * view changes.
 * <p/>
 * The third mode is for out-of-band control messages (activated by SWITCH_OUT_OF_BAND): this mode does
 * neither employ view IDs, nor does it use the same seqnos as NAK and NAK_ACK. It uses its own seqnos,
 * unrelated to the ones used by NAK and NAK_ACK, and never resets them. In combination with the sender's
 * address, this makes every out-of-band message unique. Out-of-band messages are used for example for
 * broadcasting FLUSH messages.<p>
 * Once a mode is set, it remains in effect until exactly 1 message has been sent, afterwards the default
 * mode NAK is used again.
 * <p/>
 * The following communication between 2 peers exists (left side is initiator,
 * right side receiver): <pre>
 * <p/>
 * <p/>
 * send_out_of_band
 * -------------->       synchronous     (1)
 * <-------------
 * ack
 * <p/>
 * <p/>
 * send_nak
 * -------------->       asynchronous    (2)
 * <p/>
 * <p/>
 * send_nak_ack
 * -------------->       synchronous     (3)
 * <--------------
 * ack
 * <p/>
 * <p/>
 * retransmit
 * <--------------       asynchronous    (4)
 * <p/>
 * <p/>
 * </pre>
 * <p/>
 * When a message is sent, it will contain a header describing the type of the
 * message, and containing additional data, such as sequence number etc. When a
 * message is received, it is fed into either the OutOfBander or NAKer, depending on the
 * header's type.<p>
 * Note that in the synchronous modes, ACKs are sent for each request. If a reliable unicast protocol layer
 * exists somewhere underneath this layer, then even the ACKs are transmitted reliably, thus increasing
 * the number of messages exchanged. However, since it is envisaged that ACK/OUT_OF_BAND are not used
 * frequently, this problem is currently not addressed.
 * 
 * @author Bela Ban
 */
public class NAKACK extends Protocol {
    long[] retransmit_timeout={2000, 3000, 5000, 8000};  // time(s) to wait before requesting xmit
    NAKer naker=null;
    OutOfBander out_of_bander=null;
    ViewId vid=null;
    View view=null;
    boolean is_server=false;
    Address local_addr=null;
    List queued_msgs=new List();      // msgs for next view (vid > current vid)
    Vector members=null;                // for OutOfBander: this is the destination set to
    // send messages to
    boolean send_next_msg_out_of_band=false;
    boolean send_next_msg_acking=false;
    long rebroadcast_timeout=0;       // until all outstanding ACKs recvd (rebcasting)
    TimeScheduler timer=null;
    static final String WRAPPED_MSG_KEY="NAKACK.WRAPPED_HDR";


    /**
     * Do some initial tasks
     */
    public void init() throws Exception {
        timer=stack != null? stack.timer : null;
        if(timer == null)
            if(log.isErrorEnabled()) log.error("timer is null");
        naker=new NAKer();
        out_of_bander=new OutOfBander();
    }

    public void stop() {
        out_of_bander.stop();
        naker.stop();
    }


    public String getName() {
        return "NAKACK";
    }


    public Vector providedUpServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_MSGS_RECEIVED));
        retval.addElement(new Integer(Event.GET_MSG_DIGEST));
        retval.addElement(new Integer(Event.GET_MSGS));
        return retval;
    }


    public Vector providedDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_MSGS_RECEIVED));
        return retval;
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p>
     * <b>Do not use <code>passUp()</code> in this method as the event is passed up
     * by default by the superclass after this method returns !</b>
     */
    public void up(Event evt) {
        NakAckHeader hdr;
        Message msg, msg_copy;
        int rc;

        switch(evt.getType()) {

            case Event.SUSPECT:

                    if(log.isInfoEnabled()) log.info("received SUSPECT event (suspected member=" + evt.getArg() + ")");
                naker.suspect((Address)evt.getArg());
                out_of_bander.suspect((Address)evt.getArg());
                break;

            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                naker.stable((long[])evt.getArg());
                return; // don't pass up further (Bela Aug 7 2001)

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.GET_MSGS_RECEIVED:  // returns the highest seqnos delivered to the appl. (used by STABLE)
                long[] highest=naker.getHighestSeqnosDelivered();
                passDown(new Event(Event.GET_MSGS_RECEIVED_OK, highest));
                return; // don't pass up further (bela Aug 7 2001)

            case Event.MSG:
                synchronized(this) {
                    msg=(Message)evt.getArg();

                    // check to see if this is a wrapped msg. If yes, send an ACK
                    hdr=(NakAckHeader)msg.removeHeader(WRAPPED_MSG_KEY); // see whether it is a wrapped message
                    if(hdr != null && hdr.type == NakAckHeader.WRAPPED_MSG) { // send back an ACK to hdr.sender
                        Message ack_msg=new Message(hdr.sender, null, null);
                        NakAckHeader h=new NakAckHeader(NakAckHeader.NAK_ACK_RSP, hdr.seqno, null);
                        if(hdr.sender == null)
                            if(log.isWarnEnabled()) log.warn("WRAPPED: header's 'sender' field is null; " +
                                    "cannot send ACK !");
                        ack_msg.putHeader(getName(), h);
                        passDown(new Event(Event.MSG, ack_msg));
                    }

                    hdr=(NakAckHeader)msg.removeHeader(getName());
                    if(hdr == null)
                        break;  // pass up

                    switch(hdr.type) {

                        case NakAckHeader.NAK_ACK_MSG:
                        case NakAckHeader.NAK_MSG:
                            if(hdr.type == NakAckHeader.NAK_ACK_MSG) {  // first thing: send ACK back to sender
                                Message ack_msg=new Message(msg.getSrc(), null, null);
                                NakAckHeader h=new NakAckHeader(NakAckHeader.NAK_ACK_RSP, hdr.seqno, null);
                                ack_msg.putHeader(getName(), h);
                                passDown(new Event(Event.MSG, ack_msg));
                            }

                            // while still a client, we just pass up all messages, without checking for message
                            // view IDs or seqnos: other layers further up will discard messages not destined
                            // for us (e.g. based on view IDs).
                            // Also: store msg in queue, when view change is received, replay messages with the same
                            // vid as the new view
                            if(!is_server) {
                                msg_copy=msg.copy();                // msg without header
                                msg_copy.putHeader(getName(), hdr); // put header back on as we removed it above
                                queued_msgs.add(msg_copy);          // need a copy since passUp() will modify msg
                                passUp(new Event(Event.MSG, msg));
                                return;
                            }


                            // check for VIDs: is the message's VID the same as ours ?
                            if(vid != null && hdr.vid != null) { // only check if our vid and message's vid available
                                Address my_addr=vid.getCoordAddress(), other_addr=hdr.vid.getCoordAddress();

                                if(my_addr == null || other_addr == null) {
                                    if(log.isWarnEnabled()) log.warn("my vid or message's vid does not contain " +
                                            "a coordinator; discarding message !");
                                    return;
                                }
                                if(!my_addr.equals(other_addr)) {
                                    if(log.isWarnEnabled()) log.warn("creator of own vid (" + my_addr + ")is different from " +
                                            "creator of message's vid (" + other_addr + "); discarding message !");
                                    return;
                                }

                                rc=hdr.vid.compareTo(vid);
                                if(rc > 0) {           // message is sent in next view -> store !

                                        if(log.isInfoEnabled()) log.info("message's vid (" + hdr.vid + "#" + hdr.seqno +
                                                ") is bigger than current vid: (" + vid + ") message is queued !");
                                    msg.putHeader(getName(), hdr);  // put header back on as we removed it above
                                    queued_msgs.add(msg);
                                    return;
                                }
                                if(rc < 0) {      // message sent in prev. view -> discard !

                                        if(log.isWarnEnabled()) log.warn("message's vid (" + hdr.vid + ") is smaller than " +
                                                "current vid (" + vid + "): message <" + msg.getSrc() + ":#" +
                                                hdr.seqno + "> is discarded ! Hdr is " + hdr);
                                    return;
                                }
                                // If we made it down here, the vids are the same --> OK
                            }


                            msg.putHeader(getName(), hdr);   // stored in received_msgs, re-sent later that's why hdr is added !
                            naker.receive(hdr.seqno, msg, null);
                            return;        // naker passes message up for us !


                        case NakAckHeader.RETRANSMIT_MSG:
                            naker.retransmit(msg.getSrc(), hdr.seqno, hdr.last_seqno);
                            return;

                        case NakAckHeader.NAK_ACK_RSP:
                            naker.receiveAck(hdr.seqno, msg.getSrc());
                            return;        // discard, no need to pass up

                        case NakAckHeader.OUT_OF_BAND_MSG:
                            out_of_bander.receive(hdr.seqno, msg, hdr.stable_msgs);
                            return;        // naker passes message up for us !

                        case NakAckHeader.OUT_OF_BAND_RSP:
                            out_of_bander.receiveAck(hdr.seqno, msg.getSrc());
                            return;

                        default:
                            if(log.isErrorEnabled()) log.error("NakAck header type " + hdr.type + " not known !");
                            break;
                    }
                } //end synchronized

        }

        passUp(evt);
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p>
     * <b>Do not use <code>passDown</code> in this method as the event is passed down
     * by default by the superclass after this method returns !</b>
     */
    public void down(Event evt) {
        Message msg;

        if(log.isTraceEnabled())
            log.trace("queued_msgs has " + queued_msgs.size() + " messages " +
                    "\n\nnaker:\n" + naker.dumpContents() + "\n\nout_of_bander: " +
                    out_of_bander.dumpContents() + "\n-----------------------------\n");

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.getDest() != null && !msg.getDest().isMulticastAddress())
                    break; // unicast address: not null and not mcast, pass down unchanged

                if(send_next_msg_out_of_band) {
                    out_of_bander.send(msg);
                    send_next_msg_out_of_band=false;
                }
                else if(send_next_msg_acking) {
                    naker.setAcks(true);  // require acks when sending a msg
                    naker.send(msg);
                    naker.setAcks(false);  // don't require acks when sending a msg
                    send_next_msg_acking=false;
                }
                else
                    naker.send(msg);

                return;    // don't pass down the stack, naker does this for us !

            case Event.GET_MSG_DIGEST:
                long[] highest_seqnos=(long[])evt.getArg();
                Digest digest=naker.computeMessageDigest(highest_seqnos);
                passUp(new Event(Event.GET_MSG_DIGEST_OK, digest));
                return;

            case Event.GET_MSGS:
                List lower_seqnos=naker.getMessagesInRange((long[][])evt.getArg());
                passUp(new Event(Event.GET_MSGS_OK, lower_seqnos));
                return;

            case Event.REBROADCAST_MSGS:
                rebroadcastMsgs((Vector)evt.getArg());
                break;

            case Event.TMP_VIEW:
                Vector mbrs=((View)evt.getArg()).getMembers();
                members=mbrs != null? (Vector)mbrs.clone() : new Vector();
                break;

            case Event.VIEW_CHANGE:
                synchronized(this) {
                    view=((View)((View)evt.getArg()).clone());
                    vid=view.getVid();

                    members=(Vector)view.getMembers().clone();

                    naker.reset();
                    out_of_bander.reset();

                    is_server=true;  // check vids from now on

                    // deliver messages received previously for this view
                    if(queued_msgs.size() > 0)
                        deliverQueuedMessages();
                }
                break;

            case Event.BECOME_SERVER:
                is_server=true;
                break;

            case Event.SWITCH_NAK:
                naker.setAcks(false); // don't require acks when sending a msg
                return;                     // don't pass down any further

            case Event.SWITCH_NAK_ACK:
                send_next_msg_acking=true;
                return;                     // don't pass down any further

            case Event.SWITCH_OUT_OF_BAND:
                send_next_msg_out_of_band=true;
                return;

            case Event.GET_MSGS_RECEIVED:  // return the highest seqnos delivered (=consumed by the application)
                long[] h=naker.getHighestSeqnosDelivered();
                passUp(new Event(Event.GET_MSGS_RECEIVED_OK, h));
                break;
        }


        passDown(evt);
    }


    boolean coordinator() {
        if(members == null || members.size() < 1 || local_addr == null)
            return false;
        return local_addr.equals(members.elementAt(0));
    }

    /**
     * Rebroadcasts the messages given as arguments
     */
    void rebroadcastMsgs(Vector v) {
        Vector final_v=new Vector();
        Message m1, m2;
        NakAckHeader h1, h2;

        if(v == null) return;

        // weed out duplicates
        /** todo Check!!!!! */
        for(int i=0; i < v.size(); i++) {
            boolean present=false;
            m1=(Message)v.elementAt(i);
            h1=m1 != null? (NakAckHeader)m1.getHeader(getName()) : null;
            if(m1 == null || h1 == null) { // +++ remove
                if(log.isErrorEnabled()) log.error("message is null");
                continue;
            }

            for(int j=0; j < final_v.size(); j++) {
                m2=(Message)final_v.elementAt(j);
                h2=m2 != null? (NakAckHeader)m2.getHeader(getName()) : null;
                if(m2 == null || h2 == null) { // +++ remove
                    if(log.isErrorEnabled()) log.error("message m2 is null");
                    continue;
                }
                if(h1.seqno == h2.seqno && m1.getSrc().equals(m2.getSrc())) {
                    present=true;
                }
            }
            if(!present) final_v.addElement(m1);
        }


            if(log.isWarnEnabled()) log.warn("rebroadcasting " + final_v.size() + " messages");

        /* Now re-broadcast messages using original NakAckHeader (same seqnos, same sender !) */
        for(int i=0; i < final_v.size(); i++) {
            m1=(Message)final_v.elementAt(i);
            naker.resend(m1);
        }

        // Wait until all members have acked reception of outstanding msgs. This will empty our
        // retransmission table (AckMcastSenderWindow)
        naker.waitUntilAllAcksReceived(rebroadcast_timeout);
        passUp(new Event(Event.REBROADCAST_MSGS_OK));
    }


    /**
     * Deliver all messages in the queue where <code>msg.vid == vid</code> holds. Messages were stored
     * in the queue because their vid was greater than the current view.
     */
    void deliverQueuedMessages() {
        NakAckHeader hdr;
        Message tmpmsg;
        int rc;

        while(queued_msgs.size() > 0) {
            tmpmsg=(Message)queued_msgs.removeFromHead();
            hdr=(NakAckHeader)tmpmsg.getHeader(getName());
            rc=hdr.vid.compareTo(vid);
            if(rc == 0) {               // same vid -> OK
                up(new Event(Event.MSG, tmpmsg));
            }
            else if(rc > 0) {
                ;
            }
            else
            /** todo Maybe messages from previous vids are stored while client */
                ; // can't be the case; only messages for future views are stored !
        }
    }


    public boolean setProperties(Properties props) {
        String str;
        long[] tmp;

        str=props.getProperty("retransmit_timeout");
        if(str != null) {
            tmp=Util.parseCommaDelimitedLongs(str);
            props.remove("retransmit_timeout");
            if(tmp != null && tmp.length > 0)
                retransmit_timeout=tmp;
        }

        str=props.getProperty("rebroadcast_timeout");
        if(str != null) {
            rebroadcast_timeout=Long.parseLong(str);
            props.remove("rebroadcast_timeout");
        }

        if(props.size() > 0) {
            System.err.println("NAKACK.setProperties(): these properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    class NAKer implements Retransmitter.RetransmitCommand, AckMcastSenderWindow.RetransmitCommand {
        long seqno=0;                       // current message sequence number
        Hashtable received_msgs=new Hashtable(); // ordered by sender -> NakReceiverWindow
        Hashtable sent_msgs=new Hashtable();     // ordered by seqno (sent by me !) - Messages
        AckMcastSenderWindow sender_win=new AckMcastSenderWindow(this, timer);
        boolean acking=false;                  // require acks when sending msgs
        long deleted_up_to=0;


        // Used to periodically retransmit the last message
        LastMessageRetransmitter last_msg_xmitter=new LastMessageRetransmitter();


        private class LastMessageRetransmitter implements TimeScheduler.Task {
            boolean stopped=false;
            int num_times=2;  // number of times a message is retransmitted
            long last_xmitted_seqno=0;


            public void stop() {
                stopped=true;
            }

            public boolean cancelled() {
                return stopped;
            }

            public long nextInterval() {
                return retransmit_timeout[0];
            }


            /**
             * Periodically retransmits the last seqno to all members. If the seqno doesn't change (ie. there
             * were no new messages sent) then the retransmitter task doesn't retransmit after 'num_times' times.
             */
            public void run() {
                synchronized(sent_msgs) {
                    long prevSeqno=seqno - 1;

                    if(prevSeqno == last_xmitted_seqno) {

                            if(log.isInfoEnabled()) log.info("prevSeqno=" + prevSeqno + ", last_xmitted_seqno=" +
                                    last_xmitted_seqno + ", num_times=" + num_times);
                        if(--num_times <= 0)
                            return;
                    }
                    else {
                        num_times=3;
                        last_xmitted_seqno=prevSeqno;
                    }

                    if((prevSeqno >= 0) && (prevSeqno > deleted_up_to)) {

                            if(log.isInfoEnabled()) log.info("retransmitting last message " + prevSeqno);
                        retransmit(null, prevSeqno, prevSeqno);
                    }
                }
            }

        }


        NAKer() {
            if(timer != null)
                timer.add(last_msg_xmitter, true); // fixed-rate scheduling
            else
                if(log.isErrorEnabled()) log.error("timer is null");
        }


        long getNextSeqno() {
            return seqno++;
        }


        long getHighestSeqnoSent() {
            long highest_sent=-1;
            for(Enumeration e=sent_msgs.keys(); e.hasMoreElements();)
                highest_sent=Math.max(highest_sent, ((Long)e.nextElement()).longValue());
            return highest_sent;
        }


        /**
         * Returns an array of the highest sequence numbers consumed by the application so far,
         * its order corresponding with <code>mbrs</code>. Used by coordinator as argument when
         * sending initial FLUSH request to members
         */
        long[] getHighestSeqnosDelivered() {
            long[] highest_deliv=members != null? new long[members.size()] : null;
            Address mbr;
            NakReceiverWindow win;

            if(highest_deliv == null) return null;

            for(int i=0; i < highest_deliv.length; i++) highest_deliv[i]=-1;

            synchronized(members) {
                for(int i=0; i < members.size(); i++) {
                    mbr=(Address)members.elementAt(i);
                    win=(NakReceiverWindow)received_msgs.get(mbr);
                    if(win != null)
                        highest_deliv[i]=win.getHighestDelivered();
                }
            }
            return highest_deliv;
        }


        /**
         * Return all messages sent by us that are higher than <code>seqno</code>
         */
        List getSentMessagesHigherThan(long seqno) {
            List retval=new List();
            Long key;

            for(Enumeration e=sent_msgs.keys(); e.hasMoreElements();) {
                key=(Long)e.nextElement();
                if(key.longValue() > seqno)
                    retval.add(sent_msgs.get(key));
            }
            return retval;
        }


        /**
         * Returns a message digest: for each member P in <code>highest_seqnos</code>, the highest seqno
         * received from P is added to the digest's array. If P == this, then the highest seqno
         * <em>sent</em> is added: this makes sure that messages sent but not yet received are also
         * re-broadcast (because they are also unstable).<p>If my highest seqno for a member P is
         * higher than the one in <code>highest_seqnos</code>, then all messages from P (received or sent)
         * whose seqno is higher are added to the digest's messages. The coordinator will use all digests
         * to compute a set of messages than need to be re-broadcast to the members before installing
         * a new view.
         */
        Digest computeMessageDigest(long[] highest_seqnos) {
            Digest digest=highest_seqnos != null? new Digest(highest_seqnos.length) : null;
            Address sender;
            NakReceiverWindow win;
            List unstable_msgs;
            int own_index;
            long highest_seqno_sent=-1, highest_seqno_received=-1;

            if(digest == null) {

                    if(log.isWarnEnabled()) log.warn("highest_seqnos is null, cannot compute digest !");
                return null;
            }

            if(highest_seqnos.length != members.size()) {

                    if(log.isWarnEnabled()) log.warn("the mbrship size and the size " +
                            "of the highest_seqnos array are not equal, cannot compute digest !");
                return null;
            }

            for(int i=0; i < digest.highest_seqnos.length; i++)
                digest.highest_seqnos[i]=highest_seqnos[i];

            for(int i=0; i < highest_seqnos.length; i++) {
                sender=(Address)members.elementAt(i);
                if(sender == null) continue;
                win=(NakReceiverWindow)received_msgs.get(sender);
                if(win == null) continue;
                digest.highest_seqnos[i]=win.getHighestReceived();
                unstable_msgs=win.getMessagesHigherThan(highest_seqnos[i]);
                for(Enumeration e=unstable_msgs.elements(); e.hasMoreElements();)
                    digest.msgs.add(e.nextElement());
            }


            /** If our highest seqno <em>sent</em> is higher than the one <em>received</em>, we have to
             (a) set it in the digest and (b) add the corresponding messages **/

            own_index=members.indexOf(local_addr);
            if(own_index == -1) {

                    if(log.isWarnEnabled()) log.warn("no own address in highest_seqnos");
                return digest;
            }
            highest_seqno_received=digest.highest_seqnos[own_index];
            highest_seqno_sent=getHighestSeqnoSent();

            if(highest_seqno_sent > highest_seqno_received) {
                // (a) Set highest seqno sent in digest
                digest.highest_seqnos[own_index]=highest_seqno_sent;

                // (b) Add messages between highest_seqno_received and highest_seqno_sent
                unstable_msgs=getSentMessagesHigherThan(highest_seqno_received);
                for(Enumeration e=unstable_msgs.elements(); e.hasMoreElements();)
                    digest.msgs.add(e.nextElement());
            }

            return digest;
        }


        /**
         * For each non-null member m in <code>range</code>, get messages with sequence numbers between
         * range[m][0] and range[m][1], excluding range[m][0] and including range[m][1].
         */
        List getMessagesInRange(long[][] range) {
            List retval=new List();
            List tmp;
            NakReceiverWindow win;
            Address sender;

            for(int i=0; i < range.length; i++) {
                if(range[i] != null) {
                    sender=(Address)members.elementAt(i);
                    if(sender == null) continue;
                    win=(NakReceiverWindow)received_msgs.get(sender);
                    if(win == null) continue;
                    tmp=win.getMessagesInRange(range[i][0], range[i][1]);
                    if(tmp == null || tmp.size() < 1) continue;
                    for(Enumeration e=tmp.elements(); e.hasMoreElements();)
                        retval.add(e.nextElement());
                }
            }
            return retval;
        }


        void setAcks(boolean f) {
            acking=f;
        }


        /**
         * Vector with messages (ordered by sender) that are stable and can be discarded.
         * This applies to NAK-based sender and receivers.
         */
        void stable(long[] seqnos) {
            int index;
            long seqno;
            NakReceiverWindow recv_win;
            Address sender;

            if(members == null || local_addr == null) {
                 if(log.isWarnEnabled()) log.warn("members or local_addr are null !");
                return;
            }
            index=members.indexOf(local_addr);

            if(index < 0) {

                    if(log.isWarnEnabled()) log.warn("member " + local_addr + " not found in " + members);
                return;
            }
            seqno=seqnos[index];

                if(log.isInfoEnabled()) log.info("deleting stable messages [" +
                        deleted_up_to + " - " + seqno + "]");

            // delete sent messages that are stable (kept for retransmission requests from receivers)
            synchronized(sent_msgs) {
                for(long i=deleted_up_to; i <= seqno; i++) {
                    sent_msgs.remove(new Long(i));
                }
                deleted_up_to=seqno;
            }
            // delete received msgs that are stable
            for(int i=0; i < members.size(); i++) {
                sender=(Address)members.elementAt(i);
                recv_win=(NakReceiverWindow)received_msgs.get(sender);
                if(recv_win != null)
                    recv_win.stable(seqnos[i]);  // delete all messages with seqnos <= seqnos[i]
            }
        }


        void send(Message msg) {
            long id=getNextSeqno();
            ViewId vid_copy;

            if(vid == null)
                return;
            vid_copy=(ViewId)vid.clone(); /** todo No needs to copy vid */

            if(acking) {
                msg.putHeader(getName(), new NakAckHeader(NakAckHeader.NAK_ACK_MSG, id, vid_copy));
                sender_win.add(id, msg.copy(), (Vector)members.clone()); // msg must be copied !
            }
            else
                msg.putHeader(getName(), new NakAckHeader(NakAckHeader.NAK_MSG, id, vid_copy));

             if(log.isInfoEnabled()) log.info("sending msg #" + id);

            sent_msgs.put(new Long(id), msg.copy());
            passDown(new Event(Event.MSG, msg));
        }





        /** Re-broadcast message. Message already contains NakAckHeader (therefore also seqno).
         Wrap message (including header) in a new message and bcasts using ACKS. Every receiver
         acks the message, unwraps it to get the original message and delivers the original message
         (if not yet delivered).
         // send msgs in Vector arg again (they already have a NakAckHeader !)
         // -> use the same seq numbers
         // -> destination has to be set to null (broadcast), e.g.:
         //      dst=p (me !), src=q --> dst=null, src=q

         TODO:
         -----
         resend() has to wait until it received all ACKs from all recipients (for all msgs), or until
         members were suspected. Thus we can ensure that all members received outstanding msgs before
         we switch to a new view. Otherwise, because the switch to a new view resets NAK and ACK msg
         transmission, slow members might never receive all outstanding messages.
         */



        /**
         * 1. Set the destination address of the original msg to null
         * 2. Add a new header WRAPPED_MSG and send msg. The receiver will ACK the msg,
         * remove the header and deliver the msg
         */
        void resend(Message msg) {
            Message copy=msg.copy();
            NakAckHeader hdr=(NakAckHeader)copy.getHeader(getName());
            NakAckHeader wrapped_hdr;
            long id=hdr.seqno;

            if(vid == null) return;
            copy.setDest(null);  // broadcast, e.g. dst(p), src(q) --> dst(null), src(q)
            wrapped_hdr=new NakAckHeader(NakAckHeader.WRAPPED_MSG, hdr.seqno, hdr.vid);
            wrapped_hdr.sender=local_addr;
            copy.putHeader(WRAPPED_MSG_KEY, wrapped_hdr);
            sender_win.add(id, copy.copy(), (Vector)members.clone());
             if(log.isInfoEnabled()) log.info("resending " + copy.getHeader(getName()));
            passDown(new Event(Event.MSG, copy));
        }


        void waitUntilAllAcksReceived(long timeout) {
            sender_win.waitUntilAllAcksReceived(timeout);
        }


        void receive(long id, Message msg, Vector stable_msgs) {  /** todo Vector stable_msgs is not used in NAKer.receive() */
            Address sender=msg.getSrc();
            NakReceiverWindow win=(NakReceiverWindow)received_msgs.get(sender);
            Message msg_to_deliver;

            if(win == null) {
                win=new NakReceiverWindow(sender, this, 0);
                win.setRetransmitTimeouts(retransmit_timeout);
                received_msgs.put(sender, win);
            }

             if(log.isInfoEnabled()) log.info("received <" + sender + "#" + id + ">");

            win.add(id, msg);  // add in order, then remove and pass up as many msgs as possible
            while(true) {
                msg_to_deliver=win.remove();
                if(msg_to_deliver == null)
                    break;

                if(msg_to_deliver.getHeader(getName()) instanceof NakAckHeader)
                    msg_to_deliver.removeHeader(getName());
                passUp(new Event(Event.MSG, msg_to_deliver));
            }
        }


        void receiveAck(long id, Address sender) {

                if(log.isInfoEnabled()) log.info("received ack <-- ACK <" + sender + "#" + id + ">");
            sender_win.ack(id, sender);
        }


        /**
         * Implementation of interface AckMcastSenderWindow.RetransmitCommand.<p>
         * Called by retransmission thread of AckMcastSenderWindow. <code>msg</code> is already
         * a copy, so does not need to be copied again.
         */
        public void retransmit(long seqno, Message msg, Address dest) {

                if(log.isInfoEnabled()) log.info("retransmitting message " + seqno + " to " + dest +
                        ", header is " + msg.getHeader(getName()));

            // check whether dest is member of group. If not, discard retransmission message and
            // also remove it from sender_win (AckMcastSenderWindow)
            if(members != null) {
                if(!members.contains(dest)) {

                        if(log.isInfoEnabled()) log.info("retransmitting " + seqno + ") to " + dest + ": " + dest +
                                " is not a member; discarding retransmission and removing " +
                                dest + " from sender_win");
                    sender_win.remove(dest);
                    return;
                }
            }

            msg.setDest(dest);
            passDown(new Event(Event.MSG, msg));
        }


        /**
         * Implementation of Retransmitter.RetransmitCommand.<p>
         * Called by retransmission thread when gap is detected. Sends retr. request
         * to originator of msg
         */
        public void retransmit(long first_seqno, long last_seqno, Address sender) {

                if(log.isInfoEnabled()) log.info("retransmit([" + first_seqno + ", " + last_seqno +
                        "]) to " + sender + ", vid=" + vid);

            NakAckHeader hdr=new NakAckHeader(NakAckHeader.RETRANSMIT_MSG, first_seqno, (ViewId)vid.clone()); /** todo Not necessary to clone vid */
            Message retransmit_msg=new Message(sender, null, null);

            hdr.last_seqno=last_seqno;
            retransmit_msg.putHeader(getName(), hdr);
            passDown(new Event(Event.MSG, retransmit_msg));
        }


        // Retransmit from sent-table, called when RETRANSMIT message is received
        void retransmit(Address dest, long first_seqno, long last_seqno) {
            Message m, retr_msg;

            for(long i=first_seqno; i <= last_seqno; i++) {
                m=(Message)sent_msgs.get(new Long(i));
                if(m == null) {
                    if(log.isWarnEnabled()) log.warn("(to " + dest + "): message with " + "seqno=" + i + " not found !");
                    continue;
                }

                retr_msg=m.copy();
                retr_msg.setDest(dest);

                try {
                    passDown(new Event(Event.MSG, retr_msg));
                }
                catch(Exception e) {
                    if(log.isDebugEnabled()) log.debug("exception is " + e);
                }
            }
        }


        void stop() {
            if(sender_win != null)
                sender_win.stop();
        }


        void reset() {
            NakReceiverWindow win;

            // Only reset if not coord: coord may have to retransmit the VIEW_CHANGE msg to slow members,
            // since VIEW_CHANGE results in retransmitter resetting, retransmission would be killed, and the
            // slow mbr would never receive a new view (see ./design/ViewChangeRetransmission.txt)
            if(!coordinator())
                sender_win.reset();

            sent_msgs.clear();
            for(Enumeration e=received_msgs.elements(); e.hasMoreElements();) {
                win=(NakReceiverWindow)e.nextElement();
                win.reset();
            }
            received_msgs.clear();
            seqno=0;
            deleted_up_to=0;
        }


        public void suspect(Address mbr) {
            NakReceiverWindow w;

            w=(NakReceiverWindow)received_msgs.get(mbr);
            if(w != null) {
                w.reset();
                received_msgs.remove(mbr);
            }

            sender_win.suspect(mbr); // don't keep retransmitting messages to mbr
        }


        String dumpContents() {
            StringBuffer ret=new StringBuffer();

            ret.append("\nsent_msgs: " + sent_msgs.size());

            ret.append("\nreceived_msgs: ");
            for(Enumeration e=received_msgs.keys(); e.hasMoreElements();) {
                Address key=(Address)e.nextElement();
                NakReceiverWindow w=(NakReceiverWindow)received_msgs.get(key);
                ret.append("\n" + w.toString());
            }

            ret.append("\nsender_win: " + sender_win.toString());


            return ret.toString();
        }


    }


    class OutOfBander implements AckMcastSenderWindow.RetransmitCommand {
        AckMcastSenderWindow sender_win=new AckMcastSenderWindow(this, timer);
        AckMcastReceiverWindow receiver_win=new AckMcastReceiverWindow();
        long seqno=0;


        void send(Message msg) {
            long id=seqno++;
            Vector stable_msgs=sender_win.getStableMessages();
            NakAckHeader hdr;

             if(log.isInfoEnabled()) log.info("sending msg #=" + id);

            hdr=new NakAckHeader(NakAckHeader.OUT_OF_BAND_MSG, id, null);
            hdr.stable_msgs=stable_msgs;
            msg.putHeader(getName(), hdr);

            // msg needs to be copied, otherwise it will be modified by the code below
            sender_win.add(id, msg.copy(), (Vector)members.clone());

            passDown(new Event(Event.MSG, msg));
        }


        void receive(long id, Message msg, Vector stable_msgs) {
            Address sender=msg.getSrc();

            // first thing: send ACK back to sender
            Message ack_msg=new Message(msg.getSrc(), null, null);
            NakAckHeader hdr=new NakAckHeader(NakAckHeader.OUT_OF_BAND_RSP, id, null);
            ack_msg.putHeader(getName(), hdr);


             if(log.isInfoEnabled()) log.info("received <" + sender + "#" + id + ">\n");

            if(receiver_win.add(sender, id))         // not received previously
                passUp(new Event(Event.MSG, msg));

            passDown(new Event(Event.MSG, ack_msg));  // send ACK
             if(log.isInfoEnabled()) log.info("sending ack <" + sender + "#" + id + ">\n");

            if(stable_msgs != null)
                receiver_win.remove(sender, stable_msgs);
        }


        void receiveAck(long id, Address sender) {
             if(log.isInfoEnabled()) log.info("received ack <" + sender + "#" + id + ">");
            sender_win.ack(id, sender);
        }


        /**
         * Called by retransmission thread of AckMcastSenderWindow. <code>msg</code> is already
         * a copy, so does not need to be copied again. All the necessary header are already present;
         * no header needs to be added ! The message is retransmitted as <em>unicast</em> !
         */
        public void retransmit(long seqno, Message msg, Address dest) {
             if(log.isInfoEnabled()) log.info("dest=" + dest + ", msg #" + seqno);
            msg.setDest(dest);
            passDown(new Event(Event.MSG, msg));
        }


        void reset() {
            sender_win.reset();       // +++ ?
            receiver_win.reset();     // +++ ?
        }


        void suspect(Address mbr) {
            sender_win.suspect(mbr);
            receiver_win.suspect(mbr);
        }


        void start() {
            sender_win.start();
        }

        void stop() {
            if(sender_win != null)
                sender_win.stop();
        }


        String dumpContents() {
            StringBuffer ret=new StringBuffer();
            ret.append("\nsender_win:\n" + sender_win.toString() +
                    "\nreceiver_win:\n" + receiver_win.toString());
            return ret.toString();
        }


    }


}
