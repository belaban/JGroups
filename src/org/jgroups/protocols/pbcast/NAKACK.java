// $Id: NAKACK.java,v 1.3 2003/12/15 23:45:11 belaban Exp $

package org.jgroups.protocols.pbcast;

import java.util.*;

import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;


/**
 * Negative AcKnowledgement layer (NAKs). Messages are assigned a monotonically increasing sequence number (seqno).
 * Receivers deliver messages ordered according to seqno and request retransmission of missing messages. Retransmitted
 * messages are bundled into bigger ones, e.g. when getting an xmit request for messages 1-10, instead of sending 10
 * unicast messages, we bundle all 10 messages into 1 and send it. However, since this protocol typically sits below
 * FRAG, we cannot count on FRAG to fragement/defragment the (possibly) large message into smaller ones. Therefore
 * we only bundle messages up to max_xmit_size bytes to prevent too large messages. For example, if the bundled message
 * size was a total of 34000 bytes, and max_xmit_size=16000, we'd send 3 messages: 2 16K and a 2K message. <em>Note that
 * max_xmit_size should be the same value as FRAG.frag_size (or smaller).</em>
 * @author Bela Ban
 */
public class NAKACK extends Protocol implements Retransmitter.RetransmitCommand {
    long[]        retransmit_timeout={600, 1200, 2400, 4800}; // time(s) to wait before requesting retransmission
    boolean       is_server=false;
    Address       local_addr=null;
    Vector        members=new Vector();
    long          seqno=0;                                   // current message sequence number (starts with 0)
    long          deleted_up_to=0;                           // keeps track of the lowest seqno GC'ed (sent_msgs)
    long          max_xmit_size=8192;                        // max size of a retransmit message (otherwise send multiple)
    int           gc_lag=20;                                 // number of msgs garbage collection lags behind

    /** Retransmit messages using multicast rather than unicast. This has the advantage that, if many
     * receivers lost a message, the sender only retransmits once. */
    boolean       use_mcast_xmit=false;
    Hashtable     received_msgs=new Hashtable();             // sender -> NakReceiverWindow
    Hashtable     sent_msgs=new Hashtable();                 // seqno (sent by me) -> Messages
    boolean       leaving=false;
    TimeScheduler timer=null;
    final String  name="NAKACK";


//    public static final HashMap xmit_stats=new HashMap(); // sender - HashMap(seqno - XmitStat)
//
//    public static class XmitStat {
//        int  num_xmits_requests=0;
//        long xmit_received;
//        long[] xmit_reqs=new long[10];
//
//        public XmitStat() {
//            for(int i=0; i < xmit_reqs.length; i++)
//                xmit_reqs[i]=0;
//            xmitRequest();
//        }
//
//        public void xmitRequest() {
//            xmit_reqs[num_xmits_requests++]=System.currentTimeMillis();
//        }
//
//        public void xmitReceived() {
//            xmit_received=System.currentTimeMillis();
//        }
//
//        public String toString() {
//            StringBuffer sb=new StringBuffer();
//            sb.append("total time: ");
//            if(xmit_received > 0)
//                sb.append(xmit_received - xmit_reqs[0]).append("\n");
//            else
//                sb.append("n/a\n");
//            sb.append(num_xmits_requests).append(" XMIT requests:\n");
//            for(int i=0; i < num_xmits_requests; i++) {
//                sb.append("#").append(i+1).append(": ").append(xmit_reqs[i]);
//                if(i-1 >= 0) {
//                    sb.append(" (diff to prev=").append(xmit_reqs[i] - xmit_reqs[i-1]);
//                }
//                sb.append("\nreceived at " ).append(xmit_received).append("\n");
//            }
//            return sb.toString();
//        }
//    }
//
//    public static String dumpXmitStats() {
//        StringBuffer sb=new StringBuffer();
//        HashMap tmp;
//        Map.Entry entry, entry2;
//        Long      seqno;
//        XmitStat  stat;
//        Address sender;
//        for(Iterator it=xmit_stats.entrySet().iterator(); it.hasNext();) {
//            entry=(Map.Entry)it.next();
//            sender=(Address)entry.getKey();
//            sb.append("\nsender=" + sender + ":\n");
//
//            tmp=(HashMap)entry.getValue();
//            for(Iterator it2=tmp.entrySet().iterator(); it2.hasNext();) {
//                entry2=(Map.Entry)it2.next();
//                seqno=(Long)entry2.getKey();
//                stat=(XmitStat)entry2.getValue();
//                sb.append(seqno).append(": ").append(stat).append("\n");
//            }
//        }
//        return sb.toString();
//    }
//
//
//    public static void addXmitRequest(Address sender, long seqno) {
//        HashMap tmp=(HashMap)xmit_stats.get(sender);
//        if(tmp == null) {
//            tmp=new HashMap();
//            xmit_stats.put(sender, tmp);
//        }
//        XmitStat stat=(XmitStat)tmp.get(new Long(seqno));
//        if(stat == null) {
//            stat=new XmitStat();
//            tmp.put(new Long(seqno), stat);
//        }
//        else {
//            stat.xmitRequest();
//        }
//    }
//
//    public static void addXmitResponse(Address sender, long seqno) {
//        HashMap tmp=(HashMap)xmit_stats.get(sender);
//        if(tmp != null) {
//            XmitStat stat=(XmitStat)tmp.get(new Long(seqno));
//            if(stat != null)
//                stat.xmitReceived();
//        }
//    }





    public NAKACK() {
    }


    public String getName() {
        return name;
    }


    public Vector providedUpServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.GET_DIGEST_STABLE));
        retval.addElement(new Integer(Event.GET_DIGEST_STATE));
        retval.addElement(new Integer(Event.SET_DIGEST));
        retval.addElement(new Integer(Event.MERGE_DIGEST));
        return retval;
    }


    public Vector providedDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.GET_DIGEST_STABLE));
        return retval;
    }


    public void start() throws Exception {
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("NAKACK.up(): timer is null");
    }

    public void stop() {
        if(timer != null) {
            try {
                timer.stop();
            }
            catch(Exception ex) {
            }
        }
        removeAll();  // clears sent_msgs and destroys all NakReceiverWindows
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p>
     * <b>Do not use <code>passDown()</code> in this method as the event is passed down
     * by default by the superclass after this method returns !</b>
     */
    public void down(Event evt) {
        Message msg;
        Digest digest;
        Vector mbrs;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.getDest() != null && !msg.getDest().isMulticastAddress())
                    break; // unicast address: not null and not mcast, pass down unchanged

                send(msg);
                return;    // don't pass down the stack

            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                stable((Digest)evt.getArg());
                return;  // do not pass down further (Bela Aug 7 2001)

            case Event.GET_DIGEST:
                digest=getDigest();
                passUp(new Event(Event.GET_DIGEST_OK, digest));
                return;

            case Event.GET_DIGEST_STABLE:
                digest=getDigestHighestDeliveredMsgs();
                passUp(new Event(Event.GET_DIGEST_STABLE_OK, digest));
                return;

            case Event.GET_DIGEST_STATE:
                digest=getDigest();
                passUp(new Event(Event.GET_DIGEST_STATE_OK, digest));
                return;

            case Event.SET_DIGEST:
                setDigest((Digest)evt.getArg());
                return;

            case Event.MERGE_DIGEST:
                mergeDigest((Digest)evt.getArg());
                return;

            case Event.CONFIG:
                passDown(evt);
                if(Trace.trace) Trace.info("NAKACK.down()", "received CONFIG event: " + evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                return;

            case Event.TMP_VIEW:
                mbrs=((View)evt.getArg()).getMembers();
                members.removeAllElements();
                members.addAll(mbrs);
                adjustReceivers();
                break;

            case Event.VIEW_CHANGE:
                mbrs=((View)evt.getArg()).getMembers();
                members.removeAllElements();
                members.addAll(mbrs);
                adjustReceivers();
                is_server=true;  // check vids from now on
                break;

            case Event.BECOME_SERVER:
                is_server=true;
                break;

            case Event.DISCONNECT:
                leaving=true;
                removeAll();
                seqno=0;
                break;
        }

        passDown(evt);
    }


    /**
     <b>Callback</b>. Called by superclass when event may be handled.<p>
     <b>Do not use <code>PassUp</code> in this method as the event is passed up
     by default by the superclass after this method returns !</b>
     */
    public void up(Event evt) {
        Object       obj;
        NakAckHeader hdr;
        Message      msg;
        Digest       digest;

        switch(evt.getType()) {

            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                stable((Digest)evt.getArg());
                return;  // do not pass up further (Bela Aug 7 2001)

            case Event.GET_DIGEST:
                digest=getDigestHighestDeliveredMsgs();
                passDown(new Event(Event.GET_DIGEST_OK, digest));
                return;

            case Event.GET_DIGEST_STABLE:
                digest=getDigestHighestDeliveredMsgs();
                passDown(new Event(Event.GET_DIGEST_STABLE_OK, digest));
                return;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.CONFIG:
                passUp(evt);
                if(Trace.trace) Trace.info("NAKACK.up()", "received CONFIG event: " + evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                return;

            case Event.MSG:
                msg=(Message)evt.getArg();
                obj=msg.getHeader(name);
                if(obj == null || !(obj instanceof NakAckHeader))
                    break;  // pass up (e.g. unicast msg)

                // discard messages while not yet server (i.e., until JOIN has returned)
                if(!is_server) {
                    if(Trace.trace)
                        Trace.debug("NAKACK.up()", "message was discarded (not yet server)");
                    return;
                }

                // Changed by bela Jan 29 2003: we must not remove the header, otherwise
                // further xmit requests will fail !
                //hdr=(NakAckHeader)msg.removeHeader(getName());
                hdr=(NakAckHeader)obj;

                switch(hdr.type) {

                    case NakAckHeader.MSG:
                        handleMessage(msg, hdr);
                        return;        // transmitter passes message up for us !

                    case NakAckHeader.XMIT_REQ:
                        if(hdr.range == null) {
                            if(Trace.trace)
                                Trace.error("NAKACK.up()", "XMIT_REQ: range of xmit msg == null; discarding request from " +
                                                           msg.getSrc());
                            return;
                        }
                        handleXmitReq(msg.getSrc(), hdr.range.low, hdr.range.high);
                        return;

                    case NakAckHeader.XMIT_RSP:
                        if(Trace.debug)
                            Trace.info("NAKACK.handleXmitRsp()", "received missing messages " + hdr.range);
                        handleXmitRsp(msg);
                        return;

                    default:
                        Trace.error("NAKACK.up()", "NakAck header type " + hdr.type + " not known !");
                        return;
                }
        }
        passUp(evt);
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

        str=props.getProperty("gc_lag");
        if(str != null) {
            gc_lag=new Integer(str).intValue();
            if(gc_lag < 1) {
                System.err.println("NAKACK.setProperties(): gc_lag has to be at least 1 (set to 10)");
                gc_lag=10;
            }
            props.remove("gc_lag");
        }

        str=props.getProperty("max_xmit_size");
        if(str != null) {
            max_xmit_size=Long.parseLong(str);
            props.remove("max_xmit_size");
        }

        str=props.getProperty("use_mcast_xmit");
        if(str != null) {
            use_mcast_xmit=new Boolean(str).booleanValue();
            props.remove("use_mcast_xmit");
        }

        if(props.size() > 0) {
            System.err.println("NAKACK.setProperties(): these properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }




    /* --------------------------------- Private Methods --------------------------------------- */

    long getNextSeqno() {
        return seqno++;
    }


    /**
     * Adds the message to the sent_msgs table and then passes it down the stack.
     * Change Bela Ban May 26 2002: we don't store a copy of the message, but a reference ! This saves us a
     * lot of memory. However, this also means that a message should not be changed after storing it in the
     * sent-table ! See protocols/DESIGN for details.
     */
    void send(Message msg) {
        long msg_id=getNextSeqno();
        if(Trace.debug) Trace.info("NAKACK.send()", "sending msg #" + msg_id);
        msg.putHeader(name, new NakAckHeader(NakAckHeader.MSG, msg_id));

        if(Trace.copy)
            sent_msgs.put(new Long(msg_id), msg.copy());
        else
            sent_msgs.put(new Long(msg_id), msg);
        passDown(new Event(Event.MSG, msg));
    }


    /**
     * Finds the corresponding NakReceiverWindow and adds the message to it (according to seqno). Then removes
     * as many messages as possible from the NRW and passes them up the stack. Discards messages from non-members.
     */
    void handleMessage(Message msg, NakAckHeader hdr) {
        NakReceiverWindow win=null;
        Message msg_to_deliver;
        Address sender;

        if(msg == null || hdr == null) {
            if(Trace.trace)
                Trace.error("NAKACK.handleMessage()", "msg or header is null");
            return;
        }
        sender=msg.getSrc();
        if(sender == null) {
            if(Trace.trace)
                Trace.error("NAKACK.handleMessage()", "sender of message is null");
            return;
        }

        if(Trace.debug)
            Trace.info("NAKACK.handleMessage()", "[" + local_addr + "] received <" + sender + "#" + hdr.seqno + ">");

        // msg is potentially re-sent later as result of XMIT_REQ reception; that's why hdr is added !

        // Changed by bela Jan 29 2003: we currently don't resend from received msgs, just from sent_msgs !
        // msg.putHeader(getName(), hdr);

        win=(NakReceiverWindow)received_msgs.get(sender);
        if(win == null) {  // discard message if there is no entry for sender
            if(leaving)
                return;
            if(Trace.trace)
                Trace.warn("NAKACK.handleMessage()", "[" + local_addr + "] discarded message from non-member " + sender);
            return;
        }
        win.add(hdr.seqno, msg);  // add in order, then remove and pass up as many msgs as possible
        while((msg_to_deliver=win.remove()) != null) {

            // Changed by bela Jan 29 2003: not needed (see above)
            //msg_to_deliver.removeHeader(getName());

            passUp(new Event(Event.MSG, msg_to_deliver));
        }
         //System.out.println("-- win is: " + win);
    }


    /**
     * Retransmit from sent-table, called when XMIT_REQ is received. Bundles all messages to be xmitted into
     * one large message and sends them back with an XMIT_RSP header. Note that since we cannot count on a
     * fragmentation layer below us, we have to make sure the message doesn't exceed max_xmit_size bytes. If
     * this is the case, we split the message into multiple, smaller-chunked messages. But in most cases this
     * still yields fewer messages than if each requested message was retransmitted separately.
     * @param dest The sender of the XMIT_REQ, we have to send the requested copy
     *             of the message to this address
     * @param first_seqno The first sequence number to be retransmitted (<= last_seqno)
     * @param last_seqno The last sequence number to be retransmitted (>= first_seqno) */
    void handleXmitReq(Address dest, long first_seqno, long last_seqno) {
        Message m, tmp;
        LinkedList list;
        long size=0, marker=first_seqno;

        if(Trace.debug)
            Trace.debug("NAKACK.handleXmitReq()", "received xmit request for " +
                                                  dest + " [" + first_seqno + " - " + last_seqno + "]");

        if(first_seqno > last_seqno) {
            Trace.error("NAKACK.handleXmitReq()", "first_seqno (" +
                                                  first_seqno + ") > last_seqno (" + last_seqno + "): not able to retransmit");
            return;
        }
        list=new LinkedList();
        for(long i=first_seqno; i <= last_seqno; i++) {
            m=(Message)sent_msgs.get(new Long(i));
            if(m == null) {
                Trace.error("NAKACK.handleXmitReq()", "(requester=" + dest + ") message with " +
                                                      "seqno=" + i + " not found in sent_msgs ! sent_msgs=" + printSentMsgs());
                continue;
            }
            size+=m.size();
            if(size >= max_xmit_size) {
                // size has reached max_xmit_size. go ahead and send message (excluding the current message)
                if(Trace.trace)
                    Trace.debug("NAKACK.handleXmitReq()", "xmitting msgs [" + marker + "-" + (i - 1) + "] to " + dest);
                sendXmitRsp(dest, (LinkedList)list.clone(), marker, i - 1);
                marker=i;
                list.clear();
                // fixed Dec 15 2003 (bela, patch from Joel Dice (dicej)), see explanantion under
                // bug report #854887
                size=m.size();
            }
            if(Trace.copy)
                tmp=m.copy();
            else
                tmp=m;
            tmp.setDest(dest);
            tmp.setSrc(local_addr);
            list.add(tmp);
        }

        if(list.size() > 0) {
            if(Trace.trace)
                Trace.debug("NAKACK.handleXmitReq()", "xmitting msgs [" + marker + "-" + last_seqno + "] to " + dest);
            sendXmitRsp(dest, (LinkedList)list.clone(), marker, last_seqno);
        }
    }


    void sendXmitRsp(Address dest, LinkedList xmit_list, long first_seqno, long last_seqno) {
        if(xmit_list == null || xmit_list.size() == 0) {
            Trace.error("NAKACK.sendXmitRsp()", "xmit_list is empty");
            return;
        }
        if(use_mcast_xmit)
            dest=null;
        Message msg=new Message(dest, null, xmit_list);
        msg.putHeader(name, new NakAckHeader(NakAckHeader.XMIT_RSP, first_seqno, last_seqno));
        passDown(new Event(Event.MSG, msg));
    }


    void handleXmitRsp(Message msg) {
        LinkedList list;
        Message    m;

        if(msg == null) {
            Trace.warn("NAKACK.handleXmitRsp()", "message is null");
            return;
        }
        try {
            list=(LinkedList)msg.getObject();
            if(list != null) {
                for(Iterator it=list.iterator(); it.hasNext();) {
                    m=(Message)it.next();
                    up(new Event(Event.MSG, m));
                }
            }
        }
        catch(Exception ex) {
            Trace.error("NAKACK.handleXmitRsp()",
                        "message did not contain a list (LinkedList) of retransmitted messages: " + ex);
        }
    }


//    void handleXmitRsp(Message msg) {
//        LinkedList   list;
//        Message      m;
//        Object       hdr;
//
//        if(msg == null) {
//            Trace.warn("NAKACK.handleXmitRsp()", "message is null");
//            return;
//        }
//        try {
//            list=(LinkedList)msg.getObject();
//            if(list != null) {
//                for(Iterator it=list.iterator(); it.hasNext();) {
//                    m=(Message)it.next();
//
//                    hdr=m.getHeader(name);
//                    if(hdr == null || !(hdr instanceof NakAckHeader)) {
//                        Trace.error("NAKACK.handleXmitRsp()", "retransmitted message does not have a NakAckHeader");
//                        continue;
//                    }
//
//                    handleMessage(msg, (NakAckHeader)hdr);
//                    // up(new Event(Event.MSG, m));
//                }
//            }
//        }
//        catch(Exception ex) {
//            Trace.error("NAKACK.handleXmitRsp()",
//                        "message did not contain a list (LinkedList) of retransmitted messages: " + ex);
//        }
//    }


    /** Remove old members from NakReceiverWindows and add new members (starting seqno=0). Essentially removes
     all entries from received_msgs that are not in <code>members</code> */
    void adjustReceivers() {
        Address sender;
        NakReceiverWindow win;

        // 1. Remove all senders in received_msgs that are not members anymore
        for(Enumeration e=received_msgs.keys(); e.hasMoreElements();) {
            sender=(Address)e.nextElement();
            if(!members.contains(sender)) {
                win=(NakReceiverWindow)received_msgs.get(sender);
                win.reset();
                if(Trace.trace)
                    Trace.info("NAKACK.adjustReceivers()", "removing " + sender +
                                                           " from received_msgs (not member anymore)");
                received_msgs.remove(sender);
            }
        }

        // 2. Add newly joined members to received_msgs (starting seqno=0)
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            if(!received_msgs.containsKey(sender)) {
                win=new NakReceiverWindow(sender, this, 0, timer);
                win.setRetransmitTimeouts(retransmit_timeout);
                received_msgs.put(sender, win);
            }
        }
    }


    /**
     Returns a message digest: for each member P the highest seqno received from P is added to the digest.
     */
    Digest getDigest() {
        Digest digest;
        Address sender;
        Range range;

        digest=new Digest(members.size());
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            range=getLowestAndHighestSeqno(sender, false);  // get the highest received seqno
            if(range == null) {
                if(Trace.trace)
                    Trace.error("NAKACK.getDigest()", "range is null");
                continue;
            }
            digest.add(sender, range.low, range.high);  // add another entry to the digest
        }
        return digest;
    }


    /**
     Returns a message digest: for each member P the highest seqno received from P <em>without a gap</em>
     is added to the digest. E.g. if the seqnos received from P are [+3 +4 +5 -6 +7 +8], then 5 will be returned.
     Also, the highest seqno <em>seen</em> is added. The max of all highest seqnos seen will be used (in STABLE)
     to determine whether the last seqno from a sender was received (see "Last Message Dropped" topic in DESIGN).
     */
    Digest getDigestHighestDeliveredMsgs() {
        Digest digest;
        Address sender;
        Range range;
        long high_seqno_seen=0;

        digest=new Digest(members.size());
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            range=getLowestAndHighestSeqno(sender, true);  // get the highest deliverable seqno
            if(range == null) {
                if(Trace.trace)
                    Trace.error("NAKACK.getDigest()", "range is null");
                continue;
            }
            high_seqno_seen=getHighSeqnoSeen(sender);
            digest.add(sender, range.low, range.high, high_seqno_seen);  // add another entry to the digest
        }
        return digest;
    }


    /**
     * Creates a NakReceiverWindow for each sender in the digest according to the sender's seqno.
     * If NRW already exists, reset it.
     */
    void setDigest(Digest d) {
        Address sender;
        NakReceiverWindow win;
        long initial_seqno;

        clear();
        if(d == null || d.senders == null) {
            Trace.error("NAKACK.setDigest()", "digest or digest.senders is null");
            return;
        }
        for(int i=0; i < d.size(); i++) {
            sender=d.senderAt(i);
            if(sender == null) {
                Trace.error("NAKACK.setDigest()", "sender at index " + i + " in digest is null");
                continue;
            }
            initial_seqno=d.highSeqnoAt(i);
            win=new NakReceiverWindow(sender, this, initial_seqno, timer);
            win.setRetransmitTimeouts(retransmit_timeout);
            received_msgs.put(sender, win);
        }
    }


    /**
     * For all members of the digest, adjust the NakReceiverWindows in the received_msgs hashtable.
     * If the member already exists, sets its seqno to be the max of the seqno and the seqno of the member
     * in the digest. If no entry exists, create one with the initial seqno set to the seqno of the
     * member in the digest.
     */
    void mergeDigest(Digest d) {
        Address sender;
        NakReceiverWindow win;
        long initial_seqno;

        if(d == null || d.senders == null) {
            Trace.error("NAKACK.mergeDigest()", "digest or digest.senders is null");
            return;
        }
        for(int i=0; i < d.size(); i++) {
            sender=d.senderAt(i);
            if(sender == null) {
                Trace.error("NAKACK.mergeDigest()", "sender at index " + i + " in digest is null");
                continue;
            }
            initial_seqno=d.highSeqnoAt(i);
            win=(NakReceiverWindow)received_msgs.get(sender);
            if(win == null) {
                win=new NakReceiverWindow(sender, this, initial_seqno, timer);
                win.setRetransmitTimeouts(retransmit_timeout);
                received_msgs.put(sender, win);
            }
            else {
                if(win.getHighestReceived() < initial_seqno) {
                    win.reset();
                    received_msgs.remove(sender);
                    win=new NakReceiverWindow(sender, this, initial_seqno, timer);
                    win.setRetransmitTimeouts(retransmit_timeout);
                    received_msgs.put(sender, win);
                }
            }
        }
    }


    /**
     * Returns the lowest seqno still in cache (so it can be retransmitted) and the highest seqno received so far.
     * @param sender The address for which the highest and lowest seqnos are to be retrieved
     * @param stop_at_gaps If true, the highest seqno *deliverable* will be returned. If false, the highest seqno
     *                     *received* will be returned. E.g. for [+3 +4 +5 -6 +7 +8], the highest_seqno_received is 8,
     *			   whereas the higheset_seqno_seen (deliverable) is 5.
     */
    Range getLowestAndHighestSeqno(Address sender, boolean stop_at_gaps) {
        Range r=null;
        NakReceiverWindow win;

        if(sender == null) {
            if(Trace.trace)
                Trace.error("NAKACK.getLowestAndHighestSeqno()", "sender is null");
            return r;
        }
        win=(NakReceiverWindow)received_msgs.get(sender);
        if(win == null) {
            Trace.error("NAKACK.getLowestAndHighestSeqno()", "sender " + sender +
                                                             " not found in received_msgs");
            return r;
        }
        if(stop_at_gaps)
            r=new Range(win.getLowestSeen(), win.getHighestSeen());       // deliverable messages (no gaps)
        else
            r=new Range(win.getLowestSeen(), win.getHighestReceived() + 1); // received messages
        return r;
    }


    /**
     * Returns the highest seqno seen from sender. E.g. if we received 1, 2, 4, 5 from P, then 5 will be returned
     * (doesn't take gaps into account). If we are the sender, we will return the highest seqno <em>sent</em> rather
     * then <em>received</em>
     */
    long getHighSeqnoSeen(Address sender) {
        NakReceiverWindow win;
        long ret=0;

        if(sender == null) {
            if(Trace.trace)
                Trace.error("NAKACK.getHighSeqnoSeen()", "sender is null");
            return ret;
        }
        if(sender.equals(local_addr))
            return seqno - 1;

        win=(NakReceiverWindow)received_msgs.get(sender);
        if(win == null) {
            Trace.error("NAKACK.getHighSeqnoSeen()", "sender " + sender + " not found in received_msgs");
            return ret;
        }
        ret=win.getHighestReceived();
        return ret;
    }


    /**
     Garbage collect messages that have been seen by all members. Update sent_msgs: for the sender P in the digest
     which is equal to the local address, garbage collect all messages <= seqno at digest[P]. Update received_msgs:
     for each sender P in the digest and its highest seqno seen SEQ, garbage collect all delivered_msgs in the
     NakReceiverWindow corresponding to P which are <= seqno at digest[P].
     */
    void stable(Digest d) {
        long seqno;
        NakReceiverWindow recv_win;
        Address sender;
        long my_highest_rcvd;        // highest seqno received in my digest for a sender P
        long stability_highest_rcvd; // highest seqno received in the stability vector for a sender P

        if(members == null || local_addr == null || d == null) {
            if(Trace.trace) Trace.warn("NAKACK.stable()", "members, local_addr or digest are null !");
            return;
        }

        if(Trace.trace)
            Trace.info("NAKACK.stable()", "received digest " + d);

        for(int i=0; i < d.size(); i++) {
            sender=d.senderAt(i);
            seqno=d.highSeqnoAt(i);
            if(sender == null)
                continue;

            // check whether the last seqno received for a sender P in the stability vector is > last seqno
            // received for P in my digest. if yes, request retransmission (see "Last Message Dropped" topic
            // in DESIGN)
            recv_win=(NakReceiverWindow)received_msgs.get(sender);
            if(recv_win != null) {
                my_highest_rcvd=recv_win.getHighestReceived();
                stability_highest_rcvd=d.highSeqnoSeenAt(i);

                if(stability_highest_rcvd >= 0 && stability_highest_rcvd > my_highest_rcvd) {
                    if(Trace.trace)
                        Trace.debug("NAKACK.stable()", "my_highest_rcvd (" + my_highest_rcvd +
                                                       ") < stability_highest_rcvd (" + stability_highest_rcvd +
                                                       "): requesting retransmission of " + sender + "#" + stability_highest_rcvd);
                    retransmit(stability_highest_rcvd, stability_highest_rcvd, sender);
                }
            }

            seqno-=gc_lag;
            if(seqno < 0)
                continue;

            if(Trace.trace) Trace.info("NAKACK.stable()", "deleting stable msgs < " + sender + "#" + seqno);

            // garbage collect from sent_msgs if sender was myself
            if(sender.equals(local_addr)) {
                if(Trace.trace)
                    Trace.info("NAKACK.stable()", "removing [" + deleted_up_to + " - " + seqno + "] from sent_msgs");
                for(long j=deleted_up_to; j <= seqno; j++)
                    sent_msgs.remove(new Long(j));
                deleted_up_to=seqno;
            }

            // delete *delivered* msgs that are stable
            // recv_win=(NakReceiverWindow)received_msgs.get(sender);
            if(recv_win != null)
                recv_win.stable(seqno);  // delete all messages with seqnos <= seqno
        }
    }



    /* ---------------------- Interface Retransmitter.RetransmitCommand ---------------------- */


    /**
     * Implementation of Retransmitter.RetransmitCommand. Called by retransmission thread
     * when gap is detected. Sends XMIT_REQ to originator of msg
     */
    public synchronized void retransmit(long first_seqno, long last_seqno, Address sender) {
        NakAckHeader hdr;
        Message retransmit_msg=new Message(sender, null, null);

        // Possibly replace with Trace.debug if too many messages. But xmits are
        // usually infrequent
        if(Trace.trace)
            Trace.info("NAKACK.retransmit()", "sending XMIT_REQ ([" + first_seqno +
                                              ", " + last_seqno + "]) to " + sender);
        //if(Trace.trace)
          //  Trace.debug("TRACE.special()", "XMIT: " + first_seqno + " - " + last_seqno + ", sender=" + sender);

        //for(long i=first_seqno; i <= last_seqno; i++) {
          //  addXmitRequest(sender, i);
        //}

        hdr=new NakAckHeader(NakAckHeader.XMIT_REQ, first_seqno, last_seqno);
        retransmit_msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, retransmit_msg));
    }


    /* ------------------- End of Interface Retransmitter.RetransmitCommand -------------------- */



    void clear() {
        NakReceiverWindow win;

        sent_msgs.clear();
        for(Enumeration e=received_msgs.elements(); e.hasMoreElements();) {
            win=(NakReceiverWindow)e.nextElement();
            win.reset();
        }
        received_msgs.clear();
    }


    void removeAll() {
        NakReceiverWindow win;
        Address key;

        sent_msgs.clear();
        for(Enumeration e=received_msgs.keys(); e.hasMoreElements();) {
            key=(Address)e.nextElement();
            win=(NakReceiverWindow)received_msgs.get(key);
            win.destroy();
        }
        received_msgs.clear();
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

        return ret.toString();
    }


    String printSentMsgs() {
        StringBuffer sb=new StringBuffer();
        for(Enumeration e=sent_msgs.keys(); e.hasMoreElements();)
            sb.append(e.nextElement() + " ");
        return sb.toString();
    }


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("frag_size")) {
            max_xmit_size=((Integer)map.get("frag_size")).intValue();
            if(Trace.trace) Trace.info("NAKACK.handleConfigEvent()", "max_xmit_size=" + max_xmit_size);
        }
    }

    /* ----------------------------- End of Private Methods ------------------------------------ */


}
