// $Id: NAKACK.java,v 1.81.2.4 2009/09/25 10:30:19 belaban Exp $

package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.Retransmitter;
import org.jgroups.util.*;

import java.io.IOException;
import java.util.*;


/**
 * Negative AcKnowledgement layer (NAKs). Messages are assigned a monotonically increasing sequence number (seqno).
 * Receivers deliver messages ordered according to seqno and request retransmission of missing messages. Retransmitted
 * messages are bundled into bigger ones, e.g. when getting an xmit request for messages 1-10, instead of sending 10
 * unicast messages, we bundle all 10 messages into 1 and send it. However, since this protocol typically sits below
 * FRAG, we cannot count on FRAG to fragement/defragment the (possibly) large message into smaller ones. Therefore we
 * only bundle messages up to max_xmit_size bytes to prevent too large messages. For example, if the bundled message
 * size was a total of 34000 bytes, and max_xmit_size=16000, we'd send 3 messages: 2 16K and a 2K message. <em>Note that
 * max_xmit_size should be the same value as FRAG.frag_size (or smaller).</em><br/> Retransmit requests are always sent
 * to the sender. If the sender dies, and not everyone has received its messages, they will be lost. In the future, this
 * may be changed to have receivers store all messages, so that retransmit requests can be answered by any member.
 * Trivial to implement, but not done yet. For most apps, the default retransmit properties are sufficient, if not use
 * vsync.
 *
 * @author Bela Ban
 */
public class NAKACK extends Protocol implements Retransmitter.RetransmitCommand, NakReceiverWindow.Listener {
    private long[]        retransmit_timeout={600, 1200, 2400, 4800}; // time(s) to wait before requesting retransmission
    private boolean       is_server=false;
    private Address       local_addr=null;
    private final Vector  members=new Vector(11);
    private View          view;
    private long          seqno=-1;                                  // current message sequence number (starts with 0)
    private long          max_xmit_size=8192;                        // max size of a retransmit message (otherwise send multiple)
    private int           gc_lag=20;                                 // number of msgs garbage collection lags behind

    /**
     * Retransmit messages using multicast rather than unicast. This has the advantage that, if many receivers lost a
     * message, the sender only retransmits once.
     */
    private boolean use_mcast_xmit=true;

    /**
     * Ask a random member for retransmission of a missing message. If set to true, discard_delivered_msgs will be
     * set to false
     */
    private boolean xmit_from_random_member=false;


    /**
     * Messages that have been received in order are sent up the stack (= delivered to the application). Delivered
     * messages are removed from NakReceiverWindow.received_msgs and moved to NakReceiverWindow.delivered_msgs, where
     * they are later garbage collected (by STABLE). Since we do retransmits only from sent messages, never
     * received or delivered messages, we can turn the moving to delivered_msgs off, so we don't keep the message
     * around, and don't need to wait for garbage collection to remove them.
     */
    private boolean discard_delivered_msgs=false;

    /** If value is > 0, the retransmit buffer is bounded: only the max_xmit_buf_size latest messages are kept,
     * older ones are discarded when the buffer size is exceeded. A value <= 0 means unbounded buffers
     */
    private int max_xmit_buf_size=0;


    /**
     * Hashtable<Address,NakReceiverWindow>. Stores received messages (keyed by sender). Note that this is no long term
     * storage; messages are just stored until they can be delivered (ie., until the correct FIFO order is established)
     */
    private final HashMap received_msgs=new HashMap(11);

    /** TreeMap<Long,Message>. Map of messages sent by me (keyed and sorted on sequence number) */
    private final TreeMap sent_msgs=new TreeMap();

    private boolean leaving=false;
    private boolean started=false;
    private TimeScheduler timer=null;
    private static final String name="NAKACK";

    private long xmit_reqs_received;
    private long xmit_reqs_sent;
    private long xmit_rsps_received;
    private long xmit_rsps_sent;
    private long missing_msgs_received;

    /** Captures stats on XMIT_REQS, XMIT_RSPS per sender */
    private HashMap sent=new HashMap();

    /** Captures stats on XMIT_REQS, XMIT_RSPS per receiver */
    private HashMap received=new HashMap();

    private int stats_list_size=20;

    /** BoundedList<XmitRequest>. Keeps track of the last stats_list_size XMIT requests */
    private BoundedList receive_history;

    /** BoundedList<MissingMessage>. Keeps track of the last stats_list_size missing messages received */
    private BoundedList send_history;





    public NAKACK() {
    }


    public String getName() {
        return name;
    }

    public long getXmitRequestsReceived() {return xmit_reqs_received;}
    public long getXmitRequestsSent() {return xmit_reqs_sent;}
    public long getXmitResponsesReceived() {return xmit_rsps_received;}
    public long getXmitResponsesSent() {return xmit_rsps_sent;}
    public long getMissingMessagesReceived() {return missing_msgs_received;}

    public int getPendingRetransmissionRequests() {
        int num=0;
        NakReceiverWindow win;
        synchronized(received_msgs) {
            for(Iterator it=received_msgs.values().iterator(); it.hasNext();) {
                win=(NakReceiverWindow)it.next();
                num+=win.size();
            }
        }
        return num;
    }

    public int getSentTableSize() {
        int size;
        synchronized(sent_msgs) {
            size=sent_msgs.size();
        }
        return size;
    }

    public int getReceivedTableSize() {
        int ret=0;
        NakReceiverWindow win;
        Set s=new LinkedHashSet(received_msgs.values());
        for(Iterator it=s.iterator(); it.hasNext();) {
            win=(NakReceiverWindow)it.next();
            ret+=win.size();
        }
        return ret;
    }

    public void resetStats() {
        xmit_reqs_received=xmit_reqs_sent=xmit_rsps_received=xmit_rsps_sent=missing_msgs_received=0;
        sent.clear();
        received.clear();
        if(receive_history !=null)
            receive_history.removeAll();
        if(send_history != null)
            send_history.removeAll();
    }

    public void init() throws Exception {
        if(stats) {
            send_history=new BoundedList(stats_list_size);
            receive_history=new BoundedList(stats_list_size);
        }
    }


    public int getGcLag() {
        return gc_lag;
    }

    public void setGcLag(int gc_lag) {
        this.gc_lag=gc_lag;
    }

    public boolean isUseMcastXmit() {
        return use_mcast_xmit;
    }

    public void setUseMcastXmit(boolean use_mcast_xmit) {
        this.use_mcast_xmit=use_mcast_xmit;
    }

    public boolean isXmitFromRandomMember() {
        return xmit_from_random_member;
    }

    public void setXmitFromRandomMember(boolean xmit_from_random_member) {
        this.xmit_from_random_member=xmit_from_random_member;
    }

    public boolean isDiscardDeliveredMsgs() {
        return discard_delivered_msgs;
    }

    public void setDiscardDeliveredMsgs(boolean discard_delivered_msgs) {
        this.discard_delivered_msgs=discard_delivered_msgs;
    }

    public int getMaxXmitBufSize() {
        return max_xmit_buf_size;
    }

    public void setMaxXmitBufSize(int max_xmit_buf_size) {
        this.max_xmit_buf_size=max_xmit_buf_size;
    }

    public long getMaxXmitSize() {
        return max_xmit_size;
    }

    public void setMaxXmitSize(long max_xmit_size) {
        this.max_xmit_size=max_xmit_size;
    }

    public boolean setProperties(Properties props) {
        String str;
        long[] tmp;

        super.setProperties(props);
        str=props.getProperty("retransmit_timeout");
        if(str != null) {
            tmp=Util.parseCommaDelimitedLongs(str);
            props.remove("retransmit_timeout");
            if(tmp != null && tmp.length > 0) {
                retransmit_timeout=tmp;
            }
        }

        str=props.getProperty("gc_lag");
        if(str != null) {
            gc_lag=Integer.parseInt(str);
            if(gc_lag < 0) {
                log.error("NAKACK.setProperties(): gc_lag cannot be negative, setting it to 0");
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
            use_mcast_xmit=Boolean.valueOf(str).booleanValue();
            props.remove("use_mcast_xmit");
        }

        str=props.getProperty("discard_delivered_msgs");
        if(str != null) {
            discard_delivered_msgs=Boolean.valueOf(str).booleanValue();
            props.remove("discard_delivered_msgs");
        }

        str=props.getProperty("xmit_from_random_member");
        if(str != null) {
            xmit_from_random_member=Boolean.valueOf(str).booleanValue();
            props.remove("xmit_from_random_member");
        }

        str=props.getProperty("max_xmit_buf_size");
        if(str != null) {
            max_xmit_buf_size=Integer.parseInt(str);
            props.remove("max_xmit_buf_size");
        }

        str=props.getProperty("stats_list_size");
        if(str != null) {
            stats_list_size=Integer.parseInt(str);
            props.remove("stats_list_size");
        }

        if(xmit_from_random_member) {
            if(discard_delivered_msgs) {
                discard_delivered_msgs=false;
                log.warn("xmit_from_random_member set to true: changed discard_delivered_msgs to false");
            }
        }

        if(props.size() > 0) {
            log.error("NAKACK.setProperties(): these properties are not recognized: " + props);

            return false;
        }
        return true;
    }

    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();

        retval.put("xmit_reqs_received", new Long(xmit_reqs_received));
        retval.put("xmit_reqs_sent", new Long(xmit_reqs_sent));
        retval.put("xmit_rsps_received", new Long(xmit_rsps_received));
        retval.put("xmit_rsps_sent", new Long(xmit_rsps_sent));
        retval.put("missing_msgs_received", new Long(missing_msgs_received));

        retval.put("sent_msgs", printSentMsgs());

        StringBuffer sb=new StringBuffer();
        Map.Entry entry;
        Address addr;
        Object w;
        synchronized(received_msgs) {
            for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                addr=(Address)entry.getKey();
                w=entry.getValue();
                sb.append(addr).append(": ").append(w.toString()).append('\n');
            }
        }

        retval.put("received_msgs", sb.toString());
        return retval;        
    }

    public String printStats() {
        Map.Entry entry;
        Object key, val;
        StringBuffer sb=new StringBuffer();
        sb.append("sent:\n");
        for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            if(key == null) key="<mcast dest>";
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }
        sb.append("\nreceived:\n");
        for(Iterator it=received.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }

        sb.append("\nXMIT_REQS sent:\n");
        XmitRequest tmp;
        for(Enumeration en=send_history.elements(); en.hasMoreElements();) {
            tmp=(XmitRequest)en.nextElement();
            sb.append(tmp).append("\n");
        }

        sb.append("\nMissing messages received\n");
        MissingMessage missing;
        for(Enumeration en=receive_history.elements(); en.hasMoreElements();) {
            missing=(MissingMessage)en.nextElement();
            sb.append(missing).append("\n");
        }

        return sb.toString();
    }



    public Vector providedUpServices() {
        Vector retval=new Vector(5);
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.GET_DIGEST_STABLE));
        retval.addElement(new Integer(Event.GET_DIGEST_STATE));
        retval.addElement(new Integer(Event.SET_DIGEST));
        retval.addElement(new Integer(Event.MERGE_DIGEST));
        return retval;
    }


    public Vector providedDownServices() {
        Vector retval=new Vector(2);
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.GET_DIGEST_STABLE));
        return retval;
    }


    public void start() throws Exception {
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("timer is null");
        started=true;
    }

    public void stop() {
        started=false;
        reset();  // clears sent_msgs and destroys all NakReceiverWindows
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>passDown()</code> in this
     * method as the event is passed down by default by the superclass after this method returns !</b>
     */
    public void down(Event evt) {
        Digest  digest;
        Vector  mbrs;

        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            if(dest != null && !dest.isMulticastAddress()) {
                break; // unicast address: not null and not mcast, pass down unchanged
            }
            send(evt, msg);
            return;    // don't pass down the stack

        case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
            stable((Digest)evt.getArg());
            return;  // do not pass down further (Bela Aug 7 2001)

        case Event.GET_DIGEST:
            digest=getDigest();
            passUp(new Event(Event.GET_DIGEST_OK, digest != null ? digest.copy() : null));
            return;

        case Event.GET_DIGEST_STABLE:
            digest=getDigestHighestDeliveredMsgs();
            passUp(new Event(Event.GET_DIGEST_STABLE_OK, digest != null ? digest.copy() : null));
            return;

        case Event.GET_DIGEST_STATE:
            digest=getDigest();
            passUp(new Event(Event.GET_DIGEST_STATE_OK, digest != null ? digest.copy() : null));
            return;

        case Event.SET_DIGEST:
            setDigest((Digest)evt.getArg());
            return;

        case Event.MERGE_DIGEST:
            mergeDigest((Digest)evt.getArg());
            return;

        case Event.CONFIG:
            passDown(evt);
            if(log.isDebugEnabled()) {
                log.debug("received CONFIG event: " + evt.getArg());
            }
            handleConfigEvent((HashMap)evt.getArg());
            return;

        case Event.TMP_VIEW:
            View tmp_view=(View)evt.getArg();
            mbrs=tmp_view.getMembers();
            members.clear();
            members.addAll(mbrs);
            adjustReceivers(false);
            break;

        case Event.VIEW_CHANGE:
            tmp_view=(View)evt.getArg();
            mbrs=tmp_view.getMembers();
            members.clear();
            members.addAll(mbrs);
            adjustReceivers(true);
            is_server=true;  // check vids from now on

            Set tmp=new LinkedHashSet(members);
            tmp.add(null); // for null destination (= mcast)
            sent.keySet().retainAll(tmp);
            received.keySet().retainAll(tmp);
            view=tmp_view;
            break;

        case Event.BECOME_SERVER:
            is_server=true;
            break;

        case Event.DISCONNECT:
            leaving=true;
            reset();
            break;
        }

        passDown(evt);
    }



    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>PassUp</code> in this
     * method as the event is passed up by default by the superclass after this method returns !</b>
     */
    public void up(Event evt) {
        NakAckHeader hdr;
        Message msg;
        Digest digest;

        switch(evt.getType()) {


        case Event.MSG:
            msg=(Message)evt.getArg();
            hdr=(NakAckHeader)msg.getHeader(name);
            if(hdr == null)
                break;  // pass up (e.g. unicast msg)

            // discard messages while not yet server (i.e., until JOIN has returned)
            if(!is_server) {
                if(log.isTraceEnabled())
                    log.trace("message was discarded (not yet server)");
                return;
            }

            // Changed by bela Jan 29 2003: we must not remove the header, otherwise
            // further xmit requests will fail !
            //hdr=(NakAckHeader)msg.removeHeader(getName());

            switch(hdr.type) {

            case NakAckHeader.MSG:
                handleMessage(msg, hdr);
                return;        // transmitter passes message up for us !

            case NakAckHeader.XMIT_REQ:
                if(hdr.range == null) {
                    if(log.isErrorEnabled()) {
                        log.error("XMIT_REQ: range of xmit msg is null; discarding request from " + msg.getSrc());
                    }
                    return;
                }
                handleXmitReq(msg.getSrc(), hdr.range.low, hdr.range.high, hdr.sender);
                return;

            case NakAckHeader.XMIT_RSP:
                if(log.isTraceEnabled())
                    log.trace("received missing messages " + hdr.range);
                handleXmitRsp(msg);
                return;

            default:
                if(log.isErrorEnabled()) {
                    log.error("NakAck header type " + hdr.type + " not known !");
                }
                return;
            }

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
            if(log.isDebugEnabled()) {
                log.debug("received CONFIG event: " + evt.getArg());
            }
            handleConfigEvent((HashMap)evt.getArg());
            return;
        }
        passUp(evt);
    }





    /* --------------------------------- Private Methods --------------------------------------- */

    /**
     * Adds the message to the sent_msgs table and then passes it down the stack. Change Bela Ban May 26 2002: we don't
     * store a copy of the message, but a reference ! This saves us a lot of memory. However, this also means that a
     * message should not be changed after storing it in the sent-table ! See protocols/DESIGN for details.
     * Made seqno increment and adding to sent_msgs atomic, e.g. seqno won't get incremented if adding to
     * sent_msgs fails e.g. due to an OOM (see http://jira.jboss.com/jira/browse/JGRP-179). bela Jan 13 2006
     */
    private void send(Event evt, Message msg) {
        if(msg == null)
            throw new NullPointerException("msg is null; event is " + evt);

        if(!started) {
            if(log.isWarnEnabled())
                log.warn("[" + local_addr + "] discarded message as start() has not been called, message: " + msg);
            return;
        }

        long msg_id;
        synchronized(sent_msgs) {
            try { // incrementing seqno and adding the msg to sent_msgs needs to be atomic
                msg_id=seqno +1;
                msg.putHeader(name, new NakAckHeader(NakAckHeader.MSG, msg_id));
                if(Global.copy) {
                    sent_msgs.put(new Long(msg_id), msg.copy());
                }
                else {
                    sent_msgs.put(new Long(msg_id), msg);
                }
                seqno=msg_id;
            }
            catch(Throwable t) {
                if(t instanceof Error)
                    throw (Error)t;
                if(t instanceof RuntimeException)
                    throw (RuntimeException)t;
                else {
                    throw new RuntimeException("failure adding msg " + msg + " to the retransmit table", t);
                }
            }
        }

        try { // moved passDown() out of synchronized clause (bela Sept 7 2006) http://jira.jboss.com/jira/browse/JGRP-300
            if(log.isTraceEnabled())
                log.trace("sending " + local_addr + "#" + msg_id);
            passDown(evt); // if this fails, since msg is in sent_msgs, it can be retransmitted
        }
        catch(Throwable t) { // eat the exception, don't pass it up the stack
            if(log.isWarnEnabled()) {
                log.warn("failure passing message down", t);
            }
        }
    }


    /**
     * Finds the corresponding NakReceiverWindow and adds the message to it (according to seqno). Then removes as many
     * messages as possible from the NRW and passes them up the stack. Discards messages from non-members.
     */
    private void handleMessage(Message msg, NakAckHeader hdr) {
        NakReceiverWindow win;
        Message msg_to_deliver;
        Address sender=msg.getSrc();

        if(sender == null) {
            if(log.isErrorEnabled())
                log.error("sender of message is null");
            return;
        }

        if(log.isTraceEnabled()) {
            StringBuffer sb=new StringBuffer('[');
            sb.append(local_addr).append(": received ").append(sender).append('#').append(hdr.seqno);
            log.trace(sb.toString());
        }

        // msg is potentially re-sent later as result of XMIT_REQ reception; that's why hdr is added !

        // Changed by bela Jan 29 2003: we currently don't resend from received msgs, just from sent_msgs !
        // msg.putHeader(getName(), hdr);

        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {  // discard message if there is no entry for sender
            if(leaving)
                return;
            if(log.isWarnEnabled()) {
                StringBuffer sb=new StringBuffer('[');
                sb.append(local_addr).append("] discarded message from non-member ")
                        .append(sender).append(", my view is " ).append(this.view);
                log.warn(sb);
            }
            return;
        }
        win.add(hdr.seqno, msg);  // add in order, then remove and pass up as many msgs as possible

        // Prevents concurrent passing up of messages by different threads (http://jira.jboss.com/jira/browse/JGRP-198);
        // this is all the more important once we have a threadless stack (http://jira.jboss.com/jira/browse/JGRP-181),
        // where lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time
        // We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
        // delivery of P1, Q1, Q2, P2: FIFO (implemented by NAKACK) says messages need to be delivered only in the
        // order in which they were sent by the sender
        synchronized(win) {
            while((msg_to_deliver=win.remove()) != null) {

                // Changed by bela Jan 29 2003: not needed (see above)
                //msg_to_deliver.removeHeader(getName());
                passUp(new Event(Event.MSG, msg_to_deliver));
            }
        }
    }


    /**
     * Retransmit from sent-table, called when XMIT_REQ is received. Bundles all messages to be xmitted into one large
     * message and sends them back with an XMIT_RSP header. Note that since we cannot count on a fragmentation layer
     * below us, we have to make sure the message doesn't exceed max_xmit_size bytes. If this is the case, we split the
     * message into multiple, smaller-chunked messages. But in most cases this still yields fewer messages than if each
     * requested message was retransmitted separately.
     *
     * @param xmit_requester        The sender of the XMIT_REQ, we have to send the requested copy of the message to this address
     * @param first_seqno The first sequence number to be retransmitted (<= last_seqno)
     * @param last_seqno  The last sequence number to be retransmitted (>= first_seqno)
     * @param original_sender The member who originally sent the messsage. Guaranteed to be non-null
     */
    private void handleXmitReq(Address xmit_requester, long first_seqno, long last_seqno, Address original_sender) {
        Message m, tmp;
        LinkedList list;
        long size=0, marker=first_seqno, len;
        NakReceiverWindow win=null;
        boolean      amISender; // am I the original sender ?

        if(log.isTraceEnabled()) {
            StringBuffer sb=new StringBuffer();
            sb.append(local_addr).append(": received xmit request from ").append(xmit_requester).append(" for ");
            sb.append(original_sender).append(" [").append(first_seqno).append(" - ").append(last_seqno).append("]");
            log.trace(sb.toString());
        }

        if(first_seqno > last_seqno) {
            if(log.isErrorEnabled())
                log.error("first_seqno (" + first_seqno + ") > last_seqno (" + last_seqno + "): not able to retransmit");
            return;
        }

        if(stats) {
            xmit_reqs_received+=last_seqno - first_seqno +1;
            updateStats(received, xmit_requester, 1, 0, 0);
        }

        amISender=local_addr.equals(original_sender);
        if(!amISender)
            win=(NakReceiverWindow)received_msgs.get(original_sender);

        list=new LinkedList();
        for(long i=first_seqno; i <= last_seqno; i++) {
            if(amISender) {
                m=(Message)sent_msgs.get(new Long(i)); // no need to synchronize
            }
            else {
                m=win != null? win.get(i) : null;
            }
            if(m == null) {
                if(log.isErrorEnabled()) {
                    StringBuffer sb=new StringBuffer();
                    sb.append("(requester=").append(xmit_requester).append(", local_addr=").append(this.local_addr);
                    sb.append(") message ").append(original_sender).append("::").append(i);
                    sb.append(" not found in ").append((amISender? "sent" : "received")).append(" msgs. ");
                    if(win != null) {
                        sb.append("Received messages from ").append(original_sender).append(": ").append(win.toString());
                    }
                    else {
                        sb.append("\nSent messages: ").append(printSentMsgs());
                    }
                    log.error(sb);
                }
                continue;
            }
            len=m.size();
            size+=len;
            if(size > max_xmit_size && list.size() > 0) { // changed from >= to > (yaron-r, bug #943709)
                // yaronr: added &&listSize()>0 since protocols between FRAG and NAKACK add headers, and message exceeds size.

                // size has reached max_xmit_size. go ahead and send message (excluding the current message)
                if(log.isTraceEnabled())
                    log.trace("xmitting msgs [" + marker + '-' + (i - 1) + "] to " + xmit_requester);
                sendXmitRsp(xmit_requester, (LinkedList)list.clone(), marker, i - 1);
                marker=i;
                list.clear();
                // fixed Dec 15 2003 (bela, patch from Joel Dice (dicej)), see explanantion under
                // bug report #854887
                size=len;
            }
            if(Global.copy) {
                tmp=m.copy();
            }
            else {
                tmp=m;
            }
            // tmp.setDest(xmit_requester);
            // tmp.setSrc(local_addr);
            if(tmp.getSrc() == null)
                tmp.setSrc(local_addr);
            list.add(tmp);
        }

        if(list.size() > 0) {
            if(log.isTraceEnabled())
                log.trace("xmitting msgs [" + marker + '-' + last_seqno + "] to " + xmit_requester);
            sendXmitRsp(xmit_requester, (LinkedList)list.clone(), marker, last_seqno);
            list.clear();
        }
    }

    private static void updateStats(HashMap map, Address key, int req, int rsp, int missing) {
        Entry entry=(Entry)map.get(key);
        if(entry == null) {
            entry=new Entry();
            map.put(key, entry);
        }
        entry.xmit_reqs+=req;
        entry.xmit_rsps+=rsp;
        entry.missing_msgs_rcvd+=missing;
    }

    private void sendXmitRsp(Address dest, LinkedList xmit_list, long first_seqno, long last_seqno) {
        Buffer buf;
        if(xmit_list == null || xmit_list.size() == 0) {
            if(log.isErrorEnabled())
                log.error("xmit_list is empty");
            return;
        }
        if(use_mcast_xmit)
            dest=null;

        if(stats) {
            xmit_rsps_sent+=xmit_list.size();
            updateStats(sent, dest, 0, 1, 0);
        }

        try {
            buf=Util.msgListToByteBuffer(xmit_list);
            Message msg=new Message(dest, null, buf.getBuf(), buf.getOffset(), buf.getLength());
            msg.putHeader(name, new NakAckHeader(NakAckHeader.XMIT_RSP, first_seqno, last_seqno));
            passDown(new Event(Event.MSG, msg));
        }
        catch(IOException ex) {
            log.error("failed marshalling xmit list", ex);
        }
    }


    private void handleXmitRsp(Message msg) {
        LinkedList list;
        Message m;

        if(msg == null) {
            if(log.isWarnEnabled())
                log.warn("message is null");
            return;
        }
        try {
            list=Util.byteBufferToMessageList(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            if(list != null) {
                if(stats) {
                    xmit_rsps_received+=list.size();
                    updateStats(received, msg.getSrc(), 0, 1, 0);
                }
                for(Iterator it=list.iterator(); it.hasNext();) {
                    m=(Message)it.next();
                    up(new Event(Event.MSG, m));
                }
                list.clear();
            }
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) {
                log.error("failed reading list of retransmitted messages", ex);
            }
        }
    }




    /**
     * Remove old members from NakReceiverWindows and add new members (starting seqno=0). Essentially removes all
     * entries from received_msgs that are not in <code>members</code>
     */
    private void adjustReceivers(boolean remove) {
        Address sender;
        NakReceiverWindow win;

        synchronized(received_msgs) {
            if(remove) {
                // 1. Remove all senders in received_msgs that are not members anymore
                for(Iterator it=received_msgs.keySet().iterator(); it.hasNext();) {
                    sender=(Address)it.next();
                    if(!members.contains(sender)) {
                        win=(NakReceiverWindow)received_msgs.get(sender);
                        win.reset();
                        if(log.isDebugEnabled()) {
                            log.debug("removing " + sender + " from received_msgs (not member anymore)");
                        }
                        it.remove();
                    }
                }
            }

            // 2. Add newly joined members to received_msgs (starting seqno=0)
           for(int i=0; i < members.size(); i++) {
              sender=(Address)members.elementAt(i);
              if(!received_msgs.containsKey(sender)) {
                 win=createNakReceiverWindow(sender, 0);
                 received_msgs.put(sender, win);
              }
           }
        }
    }


    /**
     * Returns a message digest: for each member P the highest seqno received from P is added to the digest.
     */
    private Digest getDigest() {
        Digest digest;
        Address sender;
        Range range;

        digest=new Digest(members.size());
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            range=getLowestAndHighestSeqno(sender, false);  // get the highest received seqno
            if(range == null) {
                if(log.isErrorEnabled()) {
                    log.error("range is null");
                }
                continue;
            }
            digest.add(sender, range.low, range.high);  // add another entry to the digest
        }
        return digest;
    }


    /**
     * Returns a message digest: for each member P the highest seqno received from P <em>without a gap</em> is added to
     * the digest. E.g. if the seqnos received from P are [+3 +4 +5 -6 +7 +8], then 5 will be returned. Also, the
     * highest seqno <em>seen</em> is added. The max of all highest seqnos seen will be used (in STABLE) to determine
     * whether the last seqno from a sender was received (see "Last Message Dropped" topic in DESIGN).
     */
    private Digest getDigestHighestDeliveredMsgs() {
        Digest digest;
        Address sender;
        Range range;
        long high_seqno_seen;

        digest=new Digest(members.size());
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            range=getLowestAndHighestSeqno(sender, true);  // get the highest deliverable seqno
            if(range == null) {
                if(log.isErrorEnabled()) {
                    log.error("range is null");
                }
                continue;
            }
            high_seqno_seen=getHighSeqnoSeen(sender);
            digest.add(sender, range.low, range.high, high_seqno_seen);  // add another entry to the digest
        }
        return digest;
    }


    /**
     * Creates a NakReceiverWindow for each sender in the digest according to the sender's seqno. If NRW already exists,
     * reset it.
     */
    private void setDigest(Digest digest) {
        if(digest == null)
            return;

        StringBuffer sb=new StringBuffer("\n[setDigest()]\n");
        sb.append("existing digest:  " + getDigest()).append("\nnew digest:       " + digest);

        for(Iterator it=digest.senders.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            Address sender=(Address)entry.getKey();
            Digest.Entry val=(Digest.Entry)entry.getValue();
            if(sender == null || val == null)
                continue;

            long initial_seqno=val.high_seqno; // val.getHighestDeliveredSeqno();
            NakReceiverWindow win=(NakReceiverWindow)received_msgs.get(sender);
            if(win != null) {
                win.reset(); // stops retransmission
                received_msgs.remove(sender);
            }
            win=createNakReceiverWindow(sender, initial_seqno);
            synchronized(received_msgs) {
                received_msgs.put(sender, win);
            }
        }
        sb.append("\n").append("resulting digest: " + getDigest());
        if(log.isDebugEnabled())
            log.debug(sb.toString());
    }


    /**
     * For all members of the digest, adjust the NakReceiverWindows in the received_msgs hashtable. If the member
     * already exists, sets its seqno to be the max of the seqno and the seqno of the member in the digest. If no entry
     * exists, create one with the initial seqno set to the seqno of the member in the digest.
     */
    private void mergeDigest(Digest d) {
        if(d == null || d.senders == null) {
            if(log.isErrorEnabled()) {
                log.error("digest or digest.senders is null");
            }
            return;
        }

        Map.Entry entry;
        Address sender;
        org.jgroups.protocols.pbcast.Digest.Entry val;
        NakReceiverWindow win;
        long initial_seqno;

        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            val=(org.jgroups.protocols.pbcast.Digest.Entry)entry.getValue();

            if(sender == null || val == null) {
                if(log.isWarnEnabled()) {
                    log.warn("sender or value is null");
                }
                continue;
            }
            initial_seqno=val.high_seqno;
            synchronized(received_msgs) {
                win=(NakReceiverWindow)received_msgs.get(sender);
                if(win == null) {
                    win=createNakReceiverWindow(sender, initial_seqno);
                    received_msgs.put(sender, win);
                }
                else {
                    // don't touch existing entries as merges should be between non-overlapping memberships !
                }
            }
        }
    }


    private NakReceiverWindow createNakReceiverWindow(Address sender, long initial_seqno) {
        NakReceiverWindow win=new NakReceiverWindow(sender, this, initial_seqno, timer);
        win.setRetransmitTimeouts(retransmit_timeout);
        win.setDiscardDeliveredMessages(discard_delivered_msgs);
        win.setMaxXmitBufSize(this.max_xmit_buf_size);
        if(stats)
            win.setListener(this);
        return win;
    }


    /**
     * Returns the lowest seqno still in cache (so it can be retransmitted) and the highest seqno received so far.
     *
     * @param sender       The address for which the highest and lowest seqnos are to be retrieved
     * @param stop_at_gaps If true, the highest seqno *deliverable* will be returned. If false, the highest seqno
     *                     *received* will be returned. E.g. for [+3 +4 +5 -6 +7 +8], the highest_seqno_received is 8,
     *                     whereas the higheset_seqno_seen (deliverable) is 5.
     */
    private Range getLowestAndHighestSeqno(Address sender, boolean stop_at_gaps) {
        Range r=null;
        NakReceiverWindow win;

        if(sender == null) {
            if(log.isErrorEnabled()) {
                log.error("sender is null");
            }
            return r;
        }
        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {
            if(log.isErrorEnabled()) {
                log.error("sender " + sender + " not found in received_msgs");
            }
            return r;
        }
        if(stop_at_gaps) {
            r=new Range(win.getLowestSeen(), win.getHighestSeen());       // deliverable messages (no gaps)
        }
        else {
            r=new Range(win.getLowestSeen(), win.getHighestReceived() + 1); // received messages
        }
        return r;
    }


    /**
     * Returns the highest seqno seen from sender. E.g. if we received 1, 2, 4, 5 from P, then 5 will be returned
     * (doesn't take gaps into account). If we are the sender, we will return the highest seqno <em>sent</em> rather
     * then <em>received</em>
     */
    private long getHighSeqnoSeen(Address sender) {
        NakReceiverWindow win;
        long ret=0;

        if(sender == null) {
            if(log.isErrorEnabled()) {
                log.error("sender is null");
            }
            return ret;
        }
        if(sender.equals(local_addr)) {
            return seqno - 1;
        }

        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {
            if(log.isErrorEnabled()) {
                log.error("sender " + sender + " not found in received_msgs");
            }
            return ret;
        }
        ret=win.getHighestReceived();
        return ret;
    }


    /**
     * Garbage collect messages that have been seen by all members. Update sent_msgs: for the sender P in the digest
     * which is equal to the local address, garbage collect all messages <= seqno at digest[P]. Update received_msgs:
     * for each sender P in the digest and its highest seqno seen SEQ, garbage collect all delivered_msgs in the
     * NakReceiverWindow corresponding to P which are <= seqno at digest[P].
     */
    private void stable(Digest d) {
        NakReceiverWindow recv_win;
        long my_highest_rcvd;        // highest seqno received in my digest for a sender P
        long stability_highest_rcvd; // highest seqno received in the stability vector for a sender P

        if(members == null || local_addr == null || d == null) {
            if(log.isWarnEnabled())
                log.warn("members, local_addr or digest are null !");
            return;
        }

        if(log.isTraceEnabled()) {
            log.trace("received stable digest " + d);
        }

        Map.Entry entry;
        Address sender;
        org.jgroups.protocols.pbcast.Digest.Entry val;
        long high_seqno_delivered, high_seqno_received;

        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            if(sender == null)
                continue;
            val=(org.jgroups.protocols.pbcast.Digest.Entry)entry.getValue();
            high_seqno_delivered=val.high_seqno;
            high_seqno_received=val.high_seqno_seen;


            // check whether the last seqno received for a sender P in the stability vector is > last seqno
            // received for P in my digest. if yes, request retransmission (see "Last Message Dropped" topic
            // in DESIGN)
            synchronized(received_msgs) {
                recv_win=(NakReceiverWindow)received_msgs.get(sender);
            }
            if(recv_win != null) {
                my_highest_rcvd=recv_win.getHighestReceived();
                stability_highest_rcvd=high_seqno_received;

                if(stability_highest_rcvd >= 0 && stability_highest_rcvd > my_highest_rcvd) {
                    if(log.isTraceEnabled()) {
                        log.trace("my_highest_rcvd (" + my_highest_rcvd + ") < stability_highest_rcvd (" +
                                stability_highest_rcvd + "): requesting retransmission of " +
                                sender + '#' + stability_highest_rcvd);
                    }
                    retransmit(stability_highest_rcvd, stability_highest_rcvd, sender);
                }
            }

            high_seqno_delivered-=gc_lag;
            if(high_seqno_delivered < 0) {
                continue;
            }

            if(log.isTraceEnabled())
                log.trace("deleting msgs <= " + high_seqno_delivered + " from " + sender);

            // garbage collect from sent_msgs if sender was myself
            if(sender.equals(local_addr)) {
                synchronized(sent_msgs) {
                    // gets us a subset from [lowest seqno - seqno]
                    SortedMap stable_keys=sent_msgs.headMap(new Long(high_seqno_delivered));
                    if(stable_keys != null) {
                        stable_keys.clear(); // this will modify sent_msgs directly
                    }
                }
            }

            // delete *delivered* msgs that are stable
            // recv_win=(NakReceiverWindow)received_msgs.get(sender);
            if(recv_win != null)
                recv_win.stable(high_seqno_delivered);  // delete all messages with seqnos <= seqno
        }
    }



    /* ---------------------- Interface Retransmitter.RetransmitCommand ---------------------- */


    /**
     * Implementation of Retransmitter.RetransmitCommand. Called by retransmission thread when gap is detected.
     */
    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        NakAckHeader hdr;
        Message retransmit_msg;
        Address dest=sender; // to whom do we send the XMIT request ?

        if(xmit_from_random_member && !local_addr.equals(sender)) {
            Address random_member=(Address)Util.pickRandomElement(members);
            if(random_member != null && !local_addr.equals(random_member)) {
                dest=random_member;
                if(log.isTraceEnabled())
                    log.trace("picked random member " + dest + " to send XMIT request to");
            }
        }

        hdr=new NakAckHeader(NakAckHeader.XMIT_REQ, first_seqno, last_seqno, sender);
        retransmit_msg=new Message(dest, null, null);
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending XMIT_REQ ([" + first_seqno + ", " + last_seqno + "]) to " + dest);
        retransmit_msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, retransmit_msg));
        if(stats) {
            xmit_reqs_sent+=last_seqno - first_seqno +1;
            updateStats(sent, dest, 1, 0, 0);
            for(long i=first_seqno; i <= last_seqno; i++) {
                XmitRequest req=new XmitRequest(sender, i, dest);
                send_history.add(req);
            }
        }
    }
    /* ------------------- End of Interface Retransmitter.RetransmitCommand -------------------- */



    /* ----------------------- Interface NakReceiverWindow.Listener ---------------------- */
    public void missingMessageReceived(long seqno, Message msg) {
        if(stats) {
            missing_msgs_received++;
            updateStats(received, msg.getSrc(), 0, 0, 1);
            MissingMessage missing=new MissingMessage(msg.getSrc(), seqno);
            receive_history.add(missing);
        }
    }
    /* ------------------- End of Interface NakReceiverWindow.Listener ------------------- */

   

    private void reset() {
        NakReceiverWindow win;

        synchronized(sent_msgs) {
            sent_msgs.clear();
            seqno=-1;
        }

        synchronized(received_msgs) {
            for(Iterator it=received_msgs.values().iterator(); it.hasNext();) {
                win=(NakReceiverWindow)it.next();
                win.destroy();
            }
            received_msgs.clear();
        }
    }


   public String printMessages() {
        StringBuffer ret=new StringBuffer();
        Map.Entry entry;
        Address addr;
        Object w;

       ret.append("\nsent_msgs: ").append(printSentMsgs());
        ret.append("\nreceived_msgs:\n");
        synchronized(received_msgs) {
            for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                addr=(Address)entry.getKey();
                w=entry.getValue();
                ret.append(addr).append(": ").append(w.toString()).append('\n');
            }
        }
        return ret.toString();
    }


    public String printSentMsgs() {
        StringBuffer sb=new StringBuffer();
        Long min_seqno, max_seqno;
        synchronized(sent_msgs) {
            min_seqno=sent_msgs.size() > 0 ? (Long)sent_msgs.firstKey() : new Long(0);
            max_seqno=sent_msgs.size() > 0 ? (Long)sent_msgs.lastKey() : new Long(0);
        }
        sb.append('[').append(min_seqno).append(" - ").append(max_seqno).append("] (").append(sent_msgs.size()).append(")");
        return sb.toString();
    }


    private void handleConfigEvent(HashMap map) {
        if(map == null) {
            return;
        }
        if(map.containsKey("frag_size")) {
            max_xmit_size=((Integer)map.get("frag_size")).intValue();
            if(log.isInfoEnabled()) {
                log.info("max_xmit_size=" + max_xmit_size);
            }
        }
    }


    static class Entry {
        long xmit_reqs, xmit_rsps, missing_msgs_rcvd;

        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(xmit_reqs).append(" xmit_reqs").append(", ").append(xmit_rsps).append(" xmit_rsps");
            sb.append(", ").append(missing_msgs_rcvd).append(" missing msgs");
            return sb.toString();
        }
    }

    static class XmitRequest {
        Address original_sender; // original sender of message
        long    seq, timestamp=System.currentTimeMillis();
        Address xmit_dest;       // destination to which XMIT_REQ is sent, usually the original sender

        XmitRequest(Address original_sender, long seqno, Address xmit_dest) {
            this.original_sender=original_sender;
            this.xmit_dest=xmit_dest;
            this.seq=seqno;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(new Date(timestamp)).append(": ").append(original_sender).append(" #").append(seq);
            sb.append(" (XMIT_REQ sent to ").append(xmit_dest).append(")");
            return sb.toString();
        }
    }

    static class MissingMessage {
        Address original_sender;
        long    seq, timestamp=System.currentTimeMillis();

        MissingMessage(Address original_sender, long seqno) {
            this.original_sender=original_sender;
            this.seq=seqno;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(new Date(timestamp)).append(": ").append(original_sender).append(" #").append(seq);
            return sb.toString();
        }
    }

    /* ----------------------------- End of Private Methods ------------------------------------ */


}
