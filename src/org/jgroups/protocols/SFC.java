package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;
import org.jgroups.util.BoundedList;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

/**
 * Simple flow control protocol. After max_credits bytes sent to the group (or an individual member), the sender blocks
 * until it receives an ack from all members (or an individual member in the case of a unicast message) that they
 * indeed received max_credits bytes. Design in doc/design/SimpleFlowControl.txt
 * @author Bela Ban
 * @version $Id: SFC.java,v 1.9 2007/01/12 14:19:42 belaban Exp $
 */
public class SFC extends Protocol {
    static final String name="SFC";

    /** Max number of bytes to send per receiver until an ack must be received before continuing sending */
    private long max_credits=2000000;

    private Long MAX_CREDITS;

    private static final Long ZERO_CREDITS=new Long(0);

    /** Current number of credits available to send */
    @GuardedBy("lock")
    private long curr_credits_available;

    /** Map which keeps track of bytes received from senders */
    @GuardedBy("received_lock")
    private final Map<Address,Long> received=new HashMap<Address,Long>(12);

    /** Set of members which have requested credits but from whom we have not yet received max_credits bytes */
    @GuardedBy("received_lock")
    private final Set<Address> pending_requesters=new HashSet<Address>();

    /** Set of members from whom we haven't yet received credits */
    @GuardedBy("lock")
    private final Set<Address> pending_creditors=new HashSet<Address>();


    private final Lock lock=new ReentrantLock();
    /** Lock protecting access to received and pending_requesters */
    private final Lock received_lock=new ReentrantLock();


    /** Used to wait for and signal when credits become available again */
    private final Condition credits_available=lock.newCondition();

    private final List<Address> members=new LinkedList<Address>();

    private boolean running=true;

    @GuardedBy("lock") long start, stop;



    // ---------------------- Management information -----------------------
    long              num_blockings=0;
    long              num_bytes_sent=0;
    long              num_credit_requests_sent=0;
    long              num_credit_requests_received=0;
    long              num_replenishments_received=0;
    long              num_replenishments_sent=0;
    long              total_block_time=0;
    final BoundedList blockings=new BoundedList(50);
    double            avg_blocking_time=0;


    public void resetStats() {
        super.resetStats();
        num_blockings=total_block_time=num_replenishments_received=num_credit_requests_sent=num_bytes_sent=0;
        num_replenishments_sent=num_credit_requests_received=0;
        avg_blocking_time=0;
        blockings.removeAll();
    }

    public long getMaxCredits() {return max_credits;}
    public long getCredits() {return curr_credits_available;}
    public long getBytesSent() {return num_bytes_sent;}
    public long getBlockings() {return num_blockings;}
    public long getCreditRequestsSent() {return num_credit_requests_sent;}
    public long getCreditRequestsReceived() {return num_credit_requests_received;}
    public long getReplenishmentsReceived() {return num_replenishments_received;}
    public long getReplenishmentsSent() {return num_replenishments_sent;}
    public long getTotalBlockingTime() {return total_block_time;}
    public double getAverageBlockingTime() {return num_blockings == 0? 0 : total_block_time / num_blockings;}


    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
        return retval;
    }

    public String printBlockingTimes() {
        return blockings.toString();
    }

    public String printReceived() {
        received_lock.lock();
        try {
            return received.toString();
        }
        finally {
            received_lock.unlock();
        }
    }

    public String printPendingCreditors() {
        lock.lock();
        try {
            return pending_creditors.toString();
        }
        finally {
            lock.unlock();
        }
    }

    public String printPendingRequesters() {
        received_lock.lock();
        try {
            return pending_requesters.toString();
        }
        finally {
            received_lock.unlock();
        }
    }

    public void unblock() {
        lock.lock();
        try {
            curr_credits_available=max_credits;
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    // ------------------- End of management information ----------------------


    public final String getName() {
        return name;
    }

    public boolean setProperties(Properties props) {
        String  str;
        super.setProperties(props);
        str=props.getProperty("max_credits");
        if(str != null) {
            max_credits=Long.parseLong(str);
            props.remove("max_credits");
        }

        Util.checkBufferSize("SFC.max_credits", max_credits);
        MAX_CREDITS=new Long(max_credits);
        curr_credits_available=max_credits;

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null && !dest.isMulticastAddress()) // only handle multicast messages
                    break;

                boolean send_credit_request=false;
                lock.lock();
                try {
                    while(curr_credits_available <=0 && running) {
                        if(trace)
                            log.trace("blocking (current credits=" + curr_credits_available + ")");
                        try {
                            num_blockings++;
                            credits_available.await(); // will be signalled when we have credit responses from all members
                        }
                        catch(InterruptedException e) {
                            if(warn)
                                log.warn("thread was interrupted", e);
                            Thread.currentThread().interrupt(); // pass the exception on to the  caller
                            return null;
                        }
                    }

                    // when we get here, curr_credits_available is guaranteed to be > 0
                    int len=msg.getLength();
                    num_bytes_sent+=len;
                    curr_credits_available-=len; // we'll block on insufficient credits on the next down() call
                    if(curr_credits_available <=0) {
                        pending_creditors.clear();
                        synchronized(members) {
                            pending_creditors.addAll(members);
                        }
                        send_credit_request=true;
                    }
                }
                finally {
                    lock.unlock();
                }

                // we don't need to protect send_credit_request because a thread above either (a) decrements the credits
                // by the msg length and sets send_credit_request to true or (b) blocks because there are no credits
                // available. So only 1 thread can ever set send_credit_request at any given time
                if(send_credit_request) {
                    if(trace)
                        log.trace("sending credit request to group");
                    start=System.nanoTime(); // only 1 thread is here at any given time
                    Object ret=down_prot.down(evt);       // send the message before the credit request
                    sendCreditRequest(); // do this outside of the lock
                    return ret;
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;
        }

        return down_prot.down(evt);
    }



    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Header hdr=(Header)msg.getHeader(name);
                Address sender=msg.getSrc();
                if(hdr != null) {
                    switch(hdr.type) {
                        case Header.CREDIT_REQUEST:
                            handleCreditRequest(sender);
                            break;
                        case Header.REPLENISH:
                            handleCreditResponse(sender);
                            break;
                        default:
                            if(log.isErrorEnabled())
                                log.error("unknown header type " + hdr.type);
                            break;
                    }
                    return null; // we don't pass the request further up
                }

                Address dest=msg.getDest();
                if(dest != null && !dest.isMulticastAddress()) // we don't handle unicast messages
                    break;

                handleMessage(msg, sender);
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }




    public void start() throws Exception {
        super.start();
        running=true;
    }


    public void stop() {
        super.stop();
        running=false;
        lock.lock();
        try {
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    private void handleMessage(Message msg, Address sender) {
        int len=msg.getLength(); // we don't care about headers, this is faster than size()

        Long new_val;
        boolean send_credit_response=false;

        received_lock.lock();
        try {
            Long credits=received.get(sender);
            if(credits == null) {
                new_val=MAX_CREDITS;
                received.put(sender, new_val);
            }
            else {
                new_val=credits.longValue() + len;
                received.put(sender, new_val);
            }
            // if(trace)
              //  log.trace("received " + len + " bytes from " + sender + ": total=" + new_val + " bytes");

            // see whether we have any pending credit requests
            if(!pending_requesters.isEmpty()
                    && pending_requesters.contains(sender)
                    && new_val.longValue() >= max_credits) {
                pending_requesters.remove(sender);
                if(trace)
                    log.trace("removed " + sender + " from credit requesters; sending credits");
                received.put(sender, ZERO_CREDITS);
                send_credit_response=true;
            }
        }
        finally {
            received_lock.unlock();
        }

        if(send_credit_response) // send outside of the monitor
            sendCreditResponse(sender);
    }
    

    private void handleCreditRequest(Address sender) {
        boolean send_credit_response=false;

        received_lock.lock();
        try {
            num_credit_requests_received++;
            Long bytes=received.get(sender);
            if(trace)
                log.trace("received credit request from " + sender + " (total received: " + bytes + " bytes");

            if(bytes == null) {
                if(log.isErrorEnabled())
                    log.error("received credit request from " + sender + ", but sender is not in received hashmap;" +
                            " adding it");
                send_credit_response=true;
            }
            else {
                if(bytes.longValue() < max_credits) {
                    if(trace)
                        log.trace("adding " + sender + " to pending credit requesters");
                    pending_requesters.add(sender);
                }
                else {
                    send_credit_response=true;
                }
            }
            if(send_credit_response)
                received.put(sender, ZERO_CREDITS);
        }
        finally{
            received_lock.unlock();
        }

        if(send_credit_response) {
            sendCreditResponse(sender);
        }
    }

    private void handleCreditResponse(Address sender) {
        lock.lock();
        try {
            num_replenishments_received++;
            if(pending_creditors.remove(sender) && pending_creditors.isEmpty()) {
                curr_credits_available=max_credits;
                stop=System.nanoTime();
                long diff=(stop-start)/1000000L;
                if(trace)
                    log.trace("replenished credits to " + curr_credits_available +
                            " (total blocking time=" + diff + " ms)");
                blockings.add(new Long(diff));
                total_block_time+=diff;
                credits_available.signalAll();
            }
        }
        finally{
            lock.unlock();
        }
    }



    private void handleViewChange(View view) {
        List<Address> mbrs=view != null? view.getMembers() : null;
        if(mbrs != null) {
            synchronized(members) {
                members.clear();
                members.addAll(mbrs);
            }
        }

        lock.lock();
        try {
            // remove all members which left from pending_creditors
            if(pending_creditors.retainAll(members) && pending_creditors.isEmpty()) {
                // the collection was changed and is empty now as a result of retainAll()
                curr_credits_available=max_credits;
                if(trace)
                    log.trace("replenished credits to " + curr_credits_available);
                credits_available.signalAll();
            }
        }
        finally {
            lock.unlock();
        }

        received_lock.lock();
        try {
            // remove left members
            received.keySet().retainAll(members);

            // add new members with *full* credits (see doc/design/SimpleFlowControl.txt for reason)
            for(Address mbr: members) {
                if(!received.containsKey(mbr))
                    received.put(mbr, MAX_CREDITS);
            }

            // remove left members from pending credit requesters
            pending_requesters.retainAll(members);
        }
        finally{
            received_lock.unlock();
        }
    }


    private void handleSuspect(Address suspected_mbr) {
        // this is the same as a credit response - we cannot block forever for a crashed member
        handleCreditResponse(suspected_mbr);
    }

    private void sendCreditRequest() {
        Message credit_req=new Message();
        // credit_req.setFlag(Message.OOB); // we need to receive the credit request after regular messages
        credit_req.putHeader(name, new Header(Header.CREDIT_REQUEST));
        num_credit_requests_sent++;
        down_prot.down(new Event(Event.MSG, credit_req));
    }

    private void sendCreditResponse(Address dest) {
        Message credit_rsp=new Message(dest);
        credit_rsp.setFlag(Message.OOB);
        Header hdr=new Header(Header.REPLENISH);
        credit_rsp.putHeader(name, hdr);
        if(trace)
            log.trace("sending credit response to " + dest);
        num_replenishments_sent++;
        down_prot.down(new Event(Event.MSG, credit_rsp));
    }



    public static class Header extends org.jgroups.Header implements Streamable {
        public static final byte CREDIT_REQUEST = 1; // the sender of the message is the requester
        public static final byte REPLENISH      = 2; // the sender of the message is the creditor

        byte  type=CREDIT_REQUEST;

        public Header() {

        }

        public Header(byte type) {
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
