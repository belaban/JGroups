package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;
import org.jgroups.util.BoundedList;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.io.*;

/**
 * Simple flow control protocol. After max_credits bytes sent to the group (or an individual member), the sender blocks
 * until it receives an ack from all members that they indeed received max_credits bytes.
 * Design in doc/design/SimpleFlowControl.txt<br/>
 * <em>Note that SFC supports only flow control for multicast messages; unicast flow control is not supported ! Use FC if
 * unicast flow control is required.</em>
 * @author Bela Ban
 * @version $Id: SFC.java,v 1.21 2008/05/08 09:46:42 vlada Exp $
 */
@MBean(description="Simple flow control protocol")
public class SFC extends Protocol {
    static final String name="SFC";

    /** Max number of bytes to send per receiver until an ack must be received before continuing sending */
    @Property
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

    /** Number of milliseconds after which we send a new credit request if we are waiting for credit responses */
    @Property
    private long max_block_time=5000;

    /** Last time a thread woke up from blocking and had to request credit */
    private long last_blocked_request=0L;

    private final List<Address> members=new LinkedList<Address>();

    private boolean running=true;

    private boolean frag_size_received=false;

    @GuardedBy("lock") long start, stop;



    // ---------------------- Management information -----------------------
    long              num_blockings=0;
    long              num_bytes_sent=0;
    long              num_credit_requests_sent=0;
    long              num_credit_requests_received=0;
    long              num_replenishments_received=0;
    long              num_replenishments_sent=0;
    long              total_block_time=0;

    final BoundedList<Long> blockings=new BoundedList<Long>(50);


    public void resetStats() {
        super.resetStats();
        num_blockings=total_block_time=num_replenishments_received=num_credit_requests_sent=num_bytes_sent=0;
        num_replenishments_sent=num_credit_requests_received=0;
        blockings.clear();
    }

    @ManagedAttribute
    public long getMaxCredits() {return max_credits;}
    @ManagedAttribute
    public long getCredits() {return curr_credits_available;}
    @ManagedAttribute
    public long getBytesSent() {return num_bytes_sent;}
    @ManagedAttribute
    public long getBlockings() {return num_blockings;}
    @ManagedAttribute
    public long getCreditRequestsSent() {return num_credit_requests_sent;}
    @ManagedAttribute
    public long getCreditRequestsReceived() {return num_credit_requests_received;}
    @ManagedAttribute
    public long getReplenishmentsReceived() {return num_replenishments_received;}
    @ManagedAttribute
    public long getReplenishmentsSent() {return num_replenishments_sent;}
    @ManagedAttribute
    public long getTotalBlockingTime() {return total_block_time;}
    @ManagedAttribute
    public double getAverageBlockingTime() {return num_blockings == 0? 0 : total_block_time / num_blockings;}


    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap<String,Object>();
        return retval;
    }

    @ManagedOperation
    public String printBlockingTimes() {
        return blockings.toString();
    }

    @ManagedOperation
    public String printReceived() {
        received_lock.lock();
        try {
            return received.toString();
        }
        finally {
            received_lock.unlock();
        }
    }
    @ManagedOperation
    public String printPendingCreditors() {
        lock.lock();
        try {
            return pending_creditors.toString();
        }
        finally {
            lock.unlock();
        }
    }
    @ManagedOperation
    public String printPendingRequesters() {
        received_lock.lock();
        try {
            return pending_requesters.toString();
        }
        finally {
            received_lock.unlock();
        }
    }
    
    @ManagedOperation
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
                        if(log.isTraceEnabled())
                            log.trace("blocking (current credits=" + curr_credits_available + ")");
                        try {
                            num_blockings++;
                            // will be signalled when we have credit responses from all members
                            boolean rc=credits_available.await(max_block_time, TimeUnit.MILLISECONDS);
                            if(rc || (curr_credits_available <=0 && running)) {
                                if(log.isTraceEnabled())
                                    log.trace("returned from await but credits still unavailable (credits=" +curr_credits_available +")");
                                long now=System.currentTimeMillis();
                                if(now - last_blocked_request >= max_block_time) {
                                    last_blocked_request=now;
                                    lock.unlock(); // send the credit request without holding the lock
                                    try {
                                        sendCreditRequest(true);
                                    }
                                    finally {
                                        lock.lock(); // now acquire the lock again
                                    }
                                }
                            }
                            else {
                                // reset the last_blocked_request stamp so the
                                // next timed out block will for sure send a request
                                last_blocked_request=0;
                            }
                        }
                        catch(InterruptedException e) {
                            // bela June 16 2007: http://jira.jboss.com/jira/browse/JGRP-536
//                            if(log.isWarnEnabled())
//                                log.warn("thread was interrupted", e);
//                            Thread.currentThread().interrupt(); // pass the exception on to the  caller
//                            return null;
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
                    if(log.isTraceEnabled())
                        log.trace("sending credit request to group");
                    start=System.nanoTime(); // only 1 thread is here at any given time
                    Object ret=down_prot.down(evt);       // send the message before the credit request
                    sendCreditRequest(false); // do this outside of the lock
                    return ret;
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;

            case Event.INFO:
                Map<String,Object> map=(Map<String,Object>)evt.getArg();
                handleInfo(map);
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
                            handleCreditRequest(sender, false);
                            break;
                        case Header.URGENT_CREDIT_REQUEST:
                            handleCreditRequest(sender, true);
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
            case Event.INFO:
                Map<String,Object> map=(Map<String,Object>)evt.getArg();
                handleInfo(map);
                break;
        }
        return up_prot.up(evt);
    }


    public void init() throws Exception{
        Util.checkBufferSize("SFC.max_credits", max_credits);
        MAX_CREDITS=new Long(max_credits);
        curr_credits_available=max_credits;
    }


    public void start() throws Exception {
        super.start();
        if(!frag_size_received) {
            log.warn("No fragmentation protocol was found. When flow control (e.g. FC or SFC) is used, we recommend " +
                    "a fragmentation protocol, due to http://jira.jboss.com/jira/browse/JGRP-590");
        }
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


    private void handleInfo(Map<String,Object> map) {
        if(map != null) {
            Integer frag_size=(Integer)map.get("frag_size");
            if(frag_size != null) {
                if(frag_size > max_credits) {
                    log.warn("The fragmentation size of the fragmentation protocol is " + frag_size +
                            ", which is greater than the max credits. While this is not incorrect, " +
                            "it may lead to long blockings. Frag size should be less than max_credits " +
                            "(http://jira.jboss.com/jira/browse/JGRP-590)");
                }
                frag_size_received=true;
            }
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
            // if(log.isTraceEnabled())
              //  log.trace("received " + len + " bytes from " + sender + ": total=" + new_val + " bytes");

            // see whether we have any pending credit requests
            if(!pending_requesters.isEmpty()
                    && pending_requesters.contains(sender)
                    && new_val.longValue() >= max_credits) {
                pending_requesters.remove(sender);
                if(log.isTraceEnabled())
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
    

    private void handleCreditRequest(Address sender, boolean urgent) {
        boolean send_credit_response=false;

        received_lock.lock();
        try {
            num_credit_requests_received++;
            Long bytes=received.get(sender);
            if(log.isTraceEnabled())
                log.trace("received credit request from " + sender + " (total received: " + bytes + " bytes");

            if(bytes == null) {
                if(log.isErrorEnabled())
                    log.error("received credit request from " + sender + ", but sender is not in received hashmap;" +
                            " adding it");
                send_credit_response=true;
            }
            else {
                if(bytes.longValue() < max_credits && !urgent) {
                    if(log.isTraceEnabled())
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
                if(log.isTraceEnabled())
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
                if(log.isTraceEnabled())
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


    private void sendCreditRequest(boolean urgent) {
        Message credit_req=new Message();
        // credit_req.setFlag(Message.OOB); // we need to receive the credit request after regular messages
        byte type=urgent? Header.URGENT_CREDIT_REQUEST : Header.CREDIT_REQUEST;
        credit_req.putHeader(name, new Header(type));
        num_credit_requests_sent++;
        down_prot.down(new Event(Event.MSG, credit_req));
    }

    private void sendCreditResponse(Address dest) {
        Message credit_rsp=new Message(dest);
        credit_rsp.setFlag(Message.OOB);
        Header hdr=new Header(Header.REPLENISH);
        credit_rsp.putHeader(name, hdr);
        if(log.isTraceEnabled())
            log.trace("sending credit response to " + dest);
        num_replenishments_sent++;
        down_prot.down(new Event(Event.MSG, credit_rsp));
    }



    public static class Header extends org.jgroups.Header implements Streamable {
        public static final byte CREDIT_REQUEST = 1; // the sender of the message is the requester
        public static final byte REPLENISH      = 2; // the sender of the message is the creditor
        public static final byte URGENT_CREDIT_REQUEST = 3;

        byte  type=CREDIT_REQUEST;

        public Header() {

        }

        public Header(byte type) {
            this.type=type;
        }

        public int size() {
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
                case URGENT_CREDIT_REQUEST: return "URGENT_CREDIT_REQUEST";
                default: return "<invalid type>";
            }
        }
    }


}
