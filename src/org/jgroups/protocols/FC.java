// $Id: FC.java,v 1.26 2005/07/01 13:05:11 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.CondVar;
import org.jgroups.util.Streamable;
import org.jgroups.util.BoundedList;

import java.io.*;
import java.util.*;

/**
 * Simple flow control protocol based on a credit system. Each sender has a number of credits (bytes
 * to send). When the credits have been exhausted, the sender blocks. Each receiver also keeps track of
 * how many credits it has received from a sender. When credits for a sender fall below a threshold,
 * the receiver sends more credits to the sender. Works for both unicast and multicast messages.<br>
 * Note that this protocol must be located towards the top of the stack, or all down_threads from JChannel to this
 * protocol must be set to false ! This is in order to block JChannel.send()/JChannel.down().
 * @author Bela Ban
 * @version $Revision: 1.26 $
 */
public class FC extends Protocol {

    /** My own address */
    Address local_addr=null;

    /** HashMap<Address,Long>: keys are members, values are credits left. For each send, the
     * number of credits is decremented by the message size */
    final HashMap sent=new HashMap(11);
    // final Map sent=new ConcurrentHashMap(11);

    /** HashMap<Address,Long>: keys are members, values are credits left (in bytes).
     * For each receive, the credits for the sender are decremented by the size of the received message.
     * When the credits are 0, we refill and send a CREDIT message to the sender. Sender blocks until CREDIT
     * is received after reaching <tt>min_credits</tt> credits. */
    final HashMap received=new HashMap(11);
    // final Map received=new ConcurrentHashMap(11);

    /** We cache the membership */
    final Vector members=new Vector(11);

    /** List of members from whom we expect credits */
    final List creditors=new ArrayList(11);

    /** Max number of bytes to send per receiver until an ack must
     * be received before continuing sending */
    long max_credits=50000;

    /** If credits fall below this limit, we send more credits to the sender. (We also send when
     * credits are exhausted (0 credits left)) */
    double min_threshold=0.25;

    /** Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation */
    long min_credits=0;

    /** Current mode. True if channel was sent a BLOCK_SEND event, false if UNBLOCK_EVENT was sent */
    CondVar blocking=new CondVar("blocking", Boolean.FALSE, sent); // we're using the sender's map as sync

    static final String name="FC";

    long start_blocking=0, stop_blocking=0;

    int num_blockings=0, num_replenishments=0;
    long total_time_blocking=0;

    final BoundedList last_blockings=new BoundedList(50);




    public String getName() {
        return name;
    }

    public void resetStats() {
        super.resetStats();
        num_blockings=num_replenishments=0;
        total_time_blocking=0;
        last_blockings.removeAll();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
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
        Object obj=blocking.get();
        return obj != null && obj instanceof Boolean && ((Boolean)obj).booleanValue();
    }

    public int getNumberOfBlockings() {
        return num_blockings;
    }

    public long getTotalTimeBlocked() {
        return total_time_blocking;
    }

    public double getAverageTimeBlocked() {
        return total_time_blocking / num_blockings;
    }

    public int getNumberOfReplenishmentsReceived() {
        return num_replenishments;
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

    public String showLastBlockingTimes() {
        return last_blockings.toString();
    }



    public void unblock() {
        synchronized(sent) {
            unblockSender();
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

        if(props.size() > 0) {
            log.error("FC.setProperties(): the following properties are not recognized: " + props);

            return false;
        }
        return true;
    }



    public void down(final Event evt) {
        switch(evt.getType()) {
        case Event.VIEW_CHANGE:
            // this has to be run in a separate thread because waitUntilEnoughCreditsAvailable() might block,
            // and the view change could potentially unblock it
            new Thread() {
                public void run() {
                    handleViewChange(((View)evt.getArg()).getMembers());
                }
            }.start();
            break;
        case Event.MSG:
            passDown(evt); // let this one go, but block on the next message if not sufficient credit
            // blocks until enought credits are available to send message
            waitUntilEnoughCreditsAvailable((Message)evt.getArg());
            return;
        }
        passDown(evt); // this could potentially use the lower protocol's thread which may block
    }




    public void up(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FcHeader hdr=(FcHeader)msg.removeHeader(name);
                if(hdr != null) {
                    if(hdr.type == FcHeader.REPLENISH) {
                        num_replenishments++;
                        handleCredit(msg.getSrc());
                        return; // don't pass message up
                    }
                }
                else {
                    adjustCredit(msg);
                }
                break;
        }
        passUp(evt);
    }



    void handleCredit(Address src) {
        if(src == null) return;
        if(log.isTraceEnabled())
            log.trace(new StringBuffer("received replenishment message from ").append(src).append(", old credit was ").
                      append(sent.get(src)).append(", new credits are ").append(max_credits).
                      append(". Creditors are\n").append(printCreditors()));
        synchronized(sent) {
            sent.put(src, new Long(max_credits));
            if(creditors.size() > 0) {  // we are blocked because we expect credit from one or more members
                removeCreditor(src);
                if(creditors.size() == 0 && blocking.get().equals(Boolean.TRUE)) {
                    unblockSender(); // triggers sent.notifyAll()...
                }
            }
        }
    }


    /**
     * Check whether sender has enough credits left. If not, send him some more
     * @param msg
     */
    void adjustCredit(Message msg) {
        Address src=msg.getSrc();
        long    size=Math.max(24, msg.getLength());
        boolean send_credits=false;

        if(src == null) {
            if(log.isErrorEnabled()) log.error("src is null");
            return;
        }

        synchronized(received) {
            // if(log.isTraceEnabled()) log.trace("credit for " + src + " is " + received.get(src));
            if(decrementCredit(received, src, size, min_credits) == false) {
                received.put(src, new Long(max_credits));
                send_credits=true; // not enough credits left
            }
        }
        if(send_credits) {
            if(log.isTraceEnabled()) log.trace("sending replenishment message to " + src);
            sendCredit(src);
        }
    }



    void sendCredit(Address dest) {
        Message  msg=new Message(dest, null, null);
        FcHeader hdr=new FcHeader(FcHeader.REPLENISH);
        msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, msg));
    }


    /**
     * Checks whether enough credits are available to send message. If not, blocks until enough credits
     * are available
     * @param evt Guaranteed to be a Message
     * @return
     */
    void waitUntilEnoughCreditsAvailable(Message msg) {
        // not enough credits, block until replenished with credits
        synchronized(sent) { // 'sent' is the same lock as blocking.getLock()...
            if(decrMessage(msg) == false) {
                if(log.isTraceEnabled())
                    log.trace("blocking due to insufficient credits, creditors=\n" + printCreditors());
                blocking.set(Boolean.TRUE);
                start_blocking=System.currentTimeMillis();
                num_blockings++;
                blocking.waitUntil(Boolean.FALSE);  // waits on 'sent'
            }
        }
    }


    /**
     * Try to decrement the credits needed for this message and return true if successful, or false otherwise.
     * For unicast destinations, the credits required are subtracted from the unicast destination member, for
     * multicast messages the credits are subtracted from all current members in the group.
     * @param msg
     * @return false: will block, true: will not block
     */
    private boolean decrMessage(Message msg) {
        Address dest;
        long    size;
        boolean success=true;

        // ******************************************************************************************************
        // this method is called by waitUntilEnoughCredits() which syncs on 'sent', so we don't need to sync here
        // ******************************************************************************************************

        if(msg == null) {
            if(log.isErrorEnabled()) log.error("msg is null");
            return true; // don't block !
        }
        dest=msg.getDest();
        size=Math.max(24, msg.getLength());
        if(dest != null && !dest.isMulticastAddress()) { // unicast destination
            // if(log.isTraceEnabled()) log.trace("credit for " + dest + " is " + sent.get(dest));
            if(decrementCredit(sent, dest, size, 0)) {
                return true;
            }
            else {
                addCreditor(dest);
                return false;
            }
        }
        else {                 // multicast destination
            for(Iterator it=members.iterator(); it.hasNext();) {
                dest=(Address)it.next();
                // if(log.isTraceEnabled()) log.trace("credit for " + dest + " is " + sent.get(dest));
                if(decrementCredit(sent, dest, size, 0) == false) {
                    addCreditor(dest);
                    success=false;
                }
            }
        }
        return success;
    }




    /** If message queueing is enabled, sends queued messages and unlocks sender (if successful) */
    private void unblockSender() {
        // **********************************************************************
        // always called with 'sent' lock acquired, so we don't need to sync here
        // **********************************************************************
        blocking.set(Boolean.FALSE);
        printBlockTime();
    }

    private void printBlockTime() {
        stop_blocking=System.currentTimeMillis();
        long diff=stop_blocking - start_blocking;
        total_time_blocking+=diff;
        last_blockings.add(new Long(diff));
        stop_blocking=start_blocking=0;
        if(log.isTraceEnabled())
            log.trace("setting blocking=false, blocking time was " + diff + "ms");
    }

    private String printCreditors() {
        // **********************************************************************
        // always called with 'sent' lock acquired, so we don't need to sync here
        // **********************************************************************
        StringBuffer sb=new StringBuffer();
        for(Iterator it=creditors.iterator(); it.hasNext();) {
            Address creditor=(Address)it.next();
            sb.append(creditor).append(": ").append(getCredits(sent, creditor)).append(" credits\n");
        }
        return sb.toString();
    }

    private void addCreditor(Address mbr) {
        if(mbr != null && !creditors.contains(mbr))
            creditors.add(mbr);
    }

    private void removeCreditor(Address mbr) {
        creditors.remove(mbr);
    }

    private long getCredits(Map map, Address mbr) {
        Long tmp=(Long)map.get(mbr);
        if(tmp == null) {
            map.put(mbr, new Long(max_credits));
            return max_credits;
        }
        return tmp.longValue();
    }





    /**
     * Find the credits associated with <tt>dest</tt> and decrement its credits by the message size.
     * @param map
     * @param dest
     * @return Whether the required credits could successfully be subtracted from the credits left
     */
//    private boolean decrementCredit(Map map, Address dest, long credits_required) {
//        long    credits_left, new_credits_left;
//        Long    tmp=(Long)map.get(dest);
//
//        if(tmp != null) {
//            credits_left=tmp.longValue();
//            new_credits_left=Math.max(0, credits_left - credits_required);
//            map.put(dest, new Long(new_credits_left));
//
//            if(new_credits_left >= min_credits + credits_required) {
//                return true;
//            }
//            else {
//                if(log.isTraceEnabled()) {
//                    StringBuffer sb=new StringBuffer();
//                    sb.append("not enough credits left for ").append(dest).append(": left=").append(new_credits_left);
//                    sb.append(", required+min_credits=").append((credits_required +min_credits)).append(", required=");
//                    sb.append(credits_required).append(", min_credits=").append(min_credits);
//                    log.trace(sb.toString());
//                }
//                return false;
//            }
//        }
//        return true;
//    }



    /**
     * Find the credits associated with <tt>dest</tt> and decrement its credits by credits_required. If the remaining
     * value is less than or equal to 0, return false, else return true. Note that we will always subtract the credits.
     * @param map
     * @param dest
     * @param credits_required Number of bytes required
     * @param minimal_credits For the receiver: add minimal credits to check whether credits need to be sent
     * @return Whether the required credits could successfully be subtracted from the credits left
     */
    private boolean decrementCredit(Map map, Address dest, long credits_required, long minimal_credits) {
        long    credits_left, new_credits_left;
        Long    tmp=(Long)map.get(dest);
        boolean success;

        if(tmp == null)
            return true;

        credits_left=tmp.longValue();
        success=credits_left > (credits_required + minimal_credits);
        new_credits_left=Math.max(0, credits_left - credits_required);
        map.put(dest, new Long(new_credits_left));

        if(success) {
            return true;
        }
        else {
            if(log.isTraceEnabled()) {
                StringBuffer sb=new StringBuffer();
                sb.append("not enough credits left for ").append(dest).append(": left=").append(new_credits_left);
                sb.append(", required+min_credits=").append((credits_required +min_credits)).append(", required=");
                sb.append(credits_required).append(", min_credits=").append(min_credits);
                log.trace(sb.toString());
            }
            return false;
        }
    }


    void handleViewChange(Vector mbrs) {
        Address addr;
        if(mbrs == null) return;

        if(log.isTraceEnabled()) log.trace("new membership: " + mbrs);
        members.clear();
        members.addAll(mbrs);

        synchronized(received) {
            // add members not in membership to received hashmap (with full credits)
            for(int i=0; i < mbrs.size(); i++) {
                addr=(Address) mbrs.elementAt(i);
                if(!received.containsKey(addr))
                    received.put(addr, new Long(max_credits));
            }
            // remove members that left
            for(Iterator it=received.keySet().iterator(); it.hasNext();) {
                addr=(Address) it.next();
                if(!mbrs.contains(addr))
                    it.remove();
            }
        }

        synchronized(sent) {
            // add members not in membership to sent hashmap (with full credits)
            for(int i=0; i < mbrs.size(); i++) {
                addr=(Address) mbrs.elementAt(i);
                if(!sent.containsKey(addr))
                    sent.put(addr, new Long(max_credits));
            }
            // remove members that left
            for(Iterator it=sent.keySet().iterator(); it.hasNext();) {
                addr=(Address)it.next();
                if(!mbrs.contains(addr))
                    it.remove(); // modified the underlying map
            }



            // remove all creditors which are not in the new view
            for(Iterator it=creditors.iterator(); it.hasNext();) {
                Address creditor=(Address) it.next();
                if(!mbrs.contains(creditor))
                    it.remove();
            }

            if(log.isTraceEnabled()) log.trace("creditors are\n" + printCreditors());
            if(creditors.size() == 0 && blocking.get().equals(Boolean.TRUE))
                unblockSender();
        }
    }

    private String printMap(Map m) {
        Map.Entry entry;
        StringBuffer sb=new StringBuffer();
        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }





    public static class FcHeader extends Header implements Streamable {
        public static final byte REPLENISH = 1;
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

    }


}
