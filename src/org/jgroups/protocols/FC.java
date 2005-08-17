// $Id: FC.java,v 1.45 2005/08/17 08:12:41 belaban Exp $

package org.jgroups.protocols;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Streamable;

import java.io.*;
import java.util.*;

/**
 * Simple flow control protocol based on a credit system. Each sender has a number of credits (bytes
 * to send). When the credits have been exhausted, the sender blocks. Each receiver also keeps track of
 * how many credits it has received from a sender. When credits for a sender fall below a threshold,
 * the receiver sends more credits to the sender. Works for both unicast and multicast messages.
 * <p>
 * Note that this protocol must be located towards the top of the stack, or all down_threads from JChannel to this
 * protocol must be set to false ! This is in order to block JChannel.send()/JChannel.down().
 * <br/>This is the second simplified implementation of the same model. The algorithm is sketched out in
 * doc/FlowControl.txt
 * @author Bela Ban
 * @version $Revision: 1.45 $
 */
public class FC extends Protocol {

    /** HashMap<Address,Long>: keys are members, values are credits left. For each send, the
     * number of credits is decremented by the message size */
    final Map sent=new HashMap(11);
    // final Map sent=new ConcurrentHashMap(11);

    /** HashMap<Address,Long>: keys are members, values are credits left (in bytes).
     * For each receive, the credits for the sender are decremented by the size of the received message.
     * When the credits are 0, we refill and send a CREDIT message to the sender. Sender blocks until CREDIT
     * is received after reaching <tt>min_credits</tt> credits. */
    final Map received=new ConcurrentReaderHashMap(11);
    // final Map received=new ConcurrentHashMap(11);


    /** List of members from whom we expect credits */
    final Set creditors=new LinkedHashSet(11);

    /** Max number of bytes to send per receiver until an ack must
     * be received before continuing sending */
    private long max_credits=50000;
    private Long max_credits_constant;

    /** Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to
     * wait forever.
     */
    private long max_block_time=5000;

    /** If credits fall below this limit, we send more credits to the sender. (We also send when
     * credits are exhausted (0 credits left)) */
    private double min_threshold=0.25;

    /** Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation */
    private long min_credits=0;

    /** Whether FC is still running, this is set to false when the protocol terminates (on stop()) */
    private boolean running=true;

    /** Determines whether or not to block on down(). Set when not enough credit is available to send a message
     * to all or a single member */
    private boolean insufficient_credit=false;

    /** the lowest credits of any destination (sent_msgs) */
    private long lowest_credit=max_credits;

    /** Mutex to block on down() */
    final Object mutex=new Object();

    static final String name="FC";

    private long start_blocking=0, stop_blocking=0;

    private int num_blockings=0;
    private int num_credit_requests_received=0, num_credit_requests_sent=0;
    private int num_credit_responses_sent=0, num_credit_responses_received=0;
    private long total_time_blocking=0;

    final BoundedList last_blockings=new BoundedList(50);

    final static FcHeader REPLENISH_HDR=new FcHeader(FcHeader.REPLENISH);
    final static FcHeader CREDIT_REQUEST_HDR=new FcHeader(FcHeader.CREDIT_REQUEST);



    public final String getName() {
        return name;
    }

    public void resetStats() {
        super.resetStats();
        num_blockings=0;
        num_credit_responses_sent=num_credit_responses_received=num_credit_requests_received=num_credit_requests_sent=0;
        total_time_blocking=0;
        last_blockings.removeAll();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
        max_credits_constant=new Long(this.max_credits);
    }

    public double getMinThreshold() {
        return min_threshold;
    }

    public void setMinThreshold(double min_threshold) {
        this.min_threshold=min_threshold;
    }

    public long getMinCredits() {
        return min_credits;
    }

    public void setMinCredits(long min_credits) {
        this.min_credits=min_credits;
    }

    public boolean isBlocked() {
        return insufficient_credit;
    }

    public int getNumberOfBlockings() {
        return num_blockings;
    }

    public long getMaxBlockTime() {
        return max_block_time;
    }

    public void setMaxBlockTime(long t) {
        max_block_time=t;
    }

    public long getTotalTimeBlocked() {
        return total_time_blocking;
    }

    public double getAverageTimeBlocked() {
        return num_blockings == 0? num_blockings : total_time_blocking / num_blockings;
    }

    public int getNumberOfCreditRequestsReceived() {
        return num_credit_requests_received;
    }

    public int getNumberOfCreditRequestsSent() {
        return num_credit_requests_sent;
    }

    public int getNumberOfCreditResponsesReceived() {
        return num_credit_responses_received;
    }

    public int getNumberOfCreditResponsesSent() {
        return num_credit_responses_sent;
    }

    public String printSenderCredits() {
        return printMap(sent);
    }

    public String printReceiverCredits() {
        return printMap(received);
    }

    public String printCredits() {
        StringBuffer sb=new StringBuffer();
        sb.append("senders:\n").append(printMap(sent)).append("\n\nreceivers:\n").append(printMap(received));
        return sb.toString();
    }

    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
        retval.put("senders", printMap(sent));
        retval.put("receivers", printMap(received));
        retval.put("num_blockings", new Integer(this.num_blockings));
        retval.put("avg_time_blocked", new Double(getAverageTimeBlocked()));
        retval.put("num_replenishments", new Integer(this.num_credit_responses_received));
        return retval;
    }

    public String showLastBlockingTimes() {
        return last_blockings.toString();
    }



    /** Allows to unblock a blocked sender from an external program, e.g. JMX */
    public void unblock() {
        synchronized(mutex) {
            if(trace)
                log.trace("unblocking the sender and replenishing all members, creditors are " + creditors);

            Map.Entry entry;
            for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                entry.setValue(max_credits_constant);
            }

            lowest_credit=computeLowestCredit(sent);
            creditors.clear();
            insufficient_credit=false;
            mutex.notifyAll();
        }
    }



    public boolean setProperties(Properties props) {
        String  str;
        boolean min_credits_set=false;

        super.setProperties(props);
        str=props.getProperty("max_credits");
        if(str != null) {
            max_credits=Long.parseLong(str);
            props.remove("max_credits");
        }

        str=props.getProperty("min_threshold");
        if(str != null) {
            min_threshold=Double.parseDouble(str);
            props.remove("min_threshold");
        }

        str=props.getProperty("min_credits");
        if(str != null) {
            min_credits=Long.parseLong(str);
            props.remove("min_credits");
            min_credits_set=true;
        }

        if(!min_credits_set)
            min_credits=(long)((double)max_credits * min_threshold);

        str=props.getProperty("max_block_time");
        if(str != null) {
            max_block_time=Long.parseLong(str);
            props.remove("max_block_time");
        }

        if(props.size() > 0) {
            log.error("FC.setProperties(): the following properties are not recognized: " + props);
            return false;
        }
        max_credits_constant=new Long(max_credits);
        return true;
    }

    public void start() throws Exception {
        super.start();
        synchronized(mutex) {
            running=true;
            insufficient_credit=false;
            lowest_credit=max_credits;
        }
    }

    public void stop() {
        super.stop();
        synchronized(mutex) {
            running=false;
            mutex.notifyAll();
        }
    }


    /**
     * We need to receive view changes concurrent to messages on the down events: a message might blocks, e.g.
     * because we don't have enough credits to send to member P. However, if member P crashed, we need to unblock !
     * @param evt
     */
    protected void receiveDownEvent(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE) {
            View v=(View)evt.getArg();
            Vector mbrs=v.getMembers();
            handleViewChange(mbrs);
        }
        super.receiveDownEvent(evt);
    }

    public void down(Event evt) {
        switch(evt.getType()) {
        case Event.MSG:
            handleDownMessage(evt);
            return;
        }
        passDown(evt); // this could potentially use the lower protocol's thread which may block
    }




    public void up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FcHeader hdr=(FcHeader)msg.removeHeader(name);
                if(hdr != null) {
                    switch(hdr.type) {
                    case FcHeader.REPLENISH:
                        num_credit_responses_received++;
                        handleCredit(msg.getSrc());
                        break;
                    case FcHeader.CREDIT_REQUEST:
                        num_credit_requests_received++;
                        Address sender=msg.getSrc();
                        if(trace)
                            log.trace("received credit request from " + sender + ": sending credits");
                        received.put(sender, max_credits_constant);
                        sendCredit(sender);
                        break;
                    default:
                        log.error("header type " + hdr.type + " not known");
                        break;
                    }
                    return; // don't pass message up
                }
                else {
                    adjustCredit(msg);
                }
                break;

        case Event.VIEW_CHANGE:
            handleViewChange(((View)evt.getArg()).getMembers());
            break;
        }
        passUp(evt);
    }


    private void handleDownMessage(Event evt) {
        Message msg=(Message)evt.getArg();
        int     length=msg.getLength();
        Address dest=msg.getDest();

        synchronized(mutex) {
            if(lowest_credit - length <= 0) {
                determineCreditors(dest, length);
                insufficient_credit=true;
                num_blockings++;
                start_blocking=System.currentTimeMillis();
                while(insufficient_credit && running) {
                    try {mutex.wait(max_block_time);} catch(InterruptedException e) {}
                    if(insufficient_credit && running) {
                        if(trace)
                            log.trace("timeout occurred waiting for credits; sending credit request to " + creditors);
                        for(Iterator it=creditors.iterator(); it.hasNext();) {
                            sendCreditRequest((Address)it.next());
                        }
                    }
                }
                stop_blocking=System.currentTimeMillis();
                long block_time=stop_blocking - start_blocking;
                if(trace)
                    log.trace("total time blocked: " + block_time + " ms");
                total_time_blocking+=block_time;
                last_blockings.add(new Long(block_time));
            }
            else {
                long tmp=decrementCredit(sent, dest, length);
                lowest_credit=Math.min(tmp, lowest_credit);
            }
        }
        // send message - either after regular processing, or after blocking (when enough credits available again)
        passDown(evt);
    }

    /**
     * Checks whether one member (unicast msg) or all members (multicast msg) have enough credits. Add those
     * that don't to the creditors list
     * @param dest
     * @param length
     */
    private void determineCreditors(Address dest, int length) {
        boolean multicast=dest == null || dest.isMulticastAddress();
        Address mbr;
        Long    credits;
        if(multicast) {
            Map.Entry entry;
            for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                mbr=(Address)entry.getKey();
                credits=(Long)entry.getValue();
                if(credits.longValue() < length)
                    creditors.add(mbr);
            }
        }
        else {
            credits=(Long)sent.get(dest);
            if(credits.longValue() < length)
                creditors.add(dest);
        }
    }


    /**
     * Decrements credits from a single member, or all members in sent_msgs, depending on whether it is a multicast
     * or unicast message. No need to acquire mutex (must already be held when this method is called)
     * @param dest
     * @param credits
     * @return The lowest number of credits left, or -1 if a unicast member was not found
     */
    private long decrementCredit(Map m, Address dest, long credits) {
        boolean multicast=dest == null || dest.isMulticastAddress();
        long    lowest=max_credits, tmp;
        Long    val;

        if(multicast) {
            if(m.size() == 0)
                return -1;
            Map.Entry entry;
            for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                val=(Long)entry.getValue();
                tmp=val.longValue();
                tmp-=credits;
                entry.setValue(new Long(tmp));
                lowest=Math.min(tmp, lowest);
            }
            return lowest;
        }
        else {
            val=(Long)m.get(dest);
            if(val != null) {
                lowest=val.longValue();
                lowest-=credits;
                m.put(dest, new Long(lowest));
                return lowest;
            }
        }
        return -1;
    }


    private void handleCredit(Address sender) {
        if(sender == null) return;
        StringBuffer sb=null;

        synchronized(mutex) {
            if(trace) {
                Long old_credit=(Long)sent.get(sender);
                sb=new StringBuffer();
                sb.append("received credit from ").append(sender).append(", old credit was ").
                        append(old_credit).append(", new credits are ").append(max_credits).
                        append(".\nCreditors before are: ").append(creditors);
            }

            sent.put(sender, max_credits_constant);
            lowest_credit=computeLowestCredit(sent);
            if(creditors.size() > 0) {  // we are blocked because we expect credit from one or more members
                creditors.remove(sender);
                if(trace) {
                    sb.append("\nCreditors after removal of ").append(sender).append(" are: ").append(creditors);
                    log.trace(sb.toString());
                }
            }
            if(insufficient_credit && lowest_credit > 0 && creditors.size() == 0) {
                insufficient_credit=false;
                mutex.notifyAll();
            }
        }
    }

    private long computeLowestCredit(Map m) {
        Collection credits=m.values(); // List of Longs (credits)
        Long retval=(Long)Collections.min(credits);
        return retval.longValue();
    }


    /**
     * Check whether sender has enough credits left. If not, send him some more
     * @param msg
     */
    private void adjustCredit(Message msg) {
        Address src=msg.getSrc();
        long    length=msg.getLength(); // we don't care about headers for the purpose of flow control

        if(src == null) {
            if(log.isErrorEnabled()) log.error("src is null");
            return;
        }

        if(length == 0)
            return; // no effect

        if(decrementCredit(received, src, length) < min_credits) {
            received.put(src, max_credits_constant);
            if(trace) log.trace("sending replenishment message to " + src);
            sendCredit(src);
        }
    }



    private void sendCredit(Address dest) {
        Message  msg=new Message(dest, null, null);
        msg.putHeader(name, REPLENISH_HDR);
        passDown(new Event(Event.MSG, msg));
        num_credit_responses_sent++;
    }

    private void sendCreditRequest(final Address dest) {
        Message  msg=new Message(dest, null, null);
        msg.putHeader(name, CREDIT_REQUEST_HDR);
        passDown(new Event(Event.MSG, msg));
        num_credit_requests_sent++;
    }


    private void handleViewChange(Vector mbrs) {
        Address addr;
        if(mbrs == null) return;
        if(trace) log.trace("new membership: " + mbrs);

        synchronized(mutex) {
            // add members not in membership to received and sent hashmap (with full credits)
            for(int i=0; i < mbrs.size(); i++) {
                addr=(Address) mbrs.elementAt(i);
                if(!received.containsKey(addr))
                    received.put(addr, max_credits_constant);
                if(!sent.containsKey(addr))
                    sent.put(addr, max_credits_constant);
            }
            // remove members that left
            for(Iterator it=received.keySet().iterator(); it.hasNext();) {
                addr=(Address) it.next();
                if(!mbrs.contains(addr))
                    it.remove();
            }

            // remove members that left
            for(Iterator it=sent.keySet().iterator(); it.hasNext();) {
                addr=(Address)it.next();
                if(!mbrs.contains(addr))
                    it.remove(); // modified the underlying map
            }

            // remove all creditors which are not in the new view
            for(Iterator it=creditors.iterator(); it.hasNext();) {
                Address creditor=(Address)it.next();
                if(!mbrs.contains(creditor))
                    it.remove();
            }

            if(trace) log.trace("creditors are " + creditors);
            if(insufficient_credit && lowest_credit > 0 && creditors.size() == 0) {
                insufficient_credit=false;
                mutex.notifyAll();
            }
        }
    }

    private static String printMap(Map m) {
        Map.Entry entry;
        StringBuffer sb=new StringBuffer();
        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }





    public static class FcHeader extends Header implements Streamable {
        public static final byte REPLENISH      = 1;
        public static final byte CREDIT_REQUEST = 2; // the sender of the message is the requester

        byte  type = REPLENISH;

        public FcHeader() {

        }

        public FcHeader(byte type) {
            this.type=type;
        }

        public long size() {
            return Global.BYTE_SIZE;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
        }

        public String toString() {
            switch(type) {
            case REPLENISH: return "REPLENISH";
            case CREDIT_REQUEST: return "CREDIT_REQUEST";
            default: return "<invalid type>";
            }
        }
    }


}
