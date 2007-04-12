package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;
import org.jgroups.util.BoundedList;

import EDU.oswego.cs.dl.util.concurrent.CondVar;
import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import java.util.*;
//import java.util.concurrent.locks.Condition;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//import java.util.concurrent.TimeUnit;
import java.io.*;

/**
 * Simple flow control protocol. After max_credits bytes sent to the group (or an individual member), the sender blocks
 * until it receives an ack from all members that they indeed received max_credits bytes.
 * Design in doc/design/SimpleFlowControl.txt
 * @author Bela Ban
 * @version $Id: SFC.java,v 1.10.2.2 2007/04/12 20:24:45 bstansberry Exp $
 */
public class SFC extends Protocol {
    static final String name="SFC";

    /** Max number of bytes to send per receiver until an ack must be received before continuing sending */
    private long max_credits=2000000;

    private Long MAX_CREDITS;

    private static final Long ZERO_CREDITS=new Long(0);

    /** Current number of credits available to send */
    private long curr_credits_available;

    /** Map which keeps track of bytes received from senders */
    private final Map received=new HashMap(12);

    /** Set of members which have requested credits but from whom we have not yet received max_credits bytes */
    private final Set pending_requesters=new HashSet();

    /** Set of members from whom we haven't yet received credits */
    private final Set pending_creditors=new HashSet();


    private final Sync lock=new ReentrantLock();
    /** Lock protecting access to received and pending_requesters */
    private final Sync received_lock=new ReentrantLock();


    /** Used to wait for and signal when credits become available again */
    private final CondVar credits_available= new CondVar(lock);

    /** Number of milliseconds after which we send a new credit request if we are waiting for credit responses */
    private long max_block_time=5000;

    private final List members=new LinkedList();

    private boolean running=true;
    
    /** Minimum interval between credit requests sent by threads that have
     * just woken up after waiting max_block_time. Used to prevent spamming 
     * the group if numerous threads block for max_block_time and then one 
     * after another awaken to request credit.
     */
    private long min_credit_request_interval = 500L;
    
    /** Last time a thread woke up from blocking and had to request credit */
    private long last_blocked_request = 0L;
    
    long start, stop;



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
      try
      {
        received_lock.acquire();
        try {
            return received.toString();
        }
        finally {
           Util.release(received_lock);
        }
      }
      catch (InterruptedException e)
      {
         String msg = "Interrupted while acquiring received_lock";
         if (warn)
            log.warn("printReceived(): " + msg);
         return msg;
      }
    }

    public String printPendingCreditors() {
       try
       {
          lock.acquire();
           try {
               return pending_creditors.toString();
           }
           finally {
              Util.release(lock);
           }
       }
       catch (InterruptedException e)
       {
          String msg = "Interrupted while acquiring lock";
          if (warn)
             log.warn("printPendingCreditors(): " + msg);
          return msg;
       }
    }

    public String printPendingRequesters() {
       try
       {
          received_lock.acquire();
           try {
               return pending_requesters.toString();
           }
           finally {
              Util.release(received_lock);
           }
       }
       catch (InterruptedException e)
       {
          String msg = "Interrupted while acquiring received_lock";
          if (warn)
             log.warn("printPendingRequesters(): " + msg);
          return msg;
       }
    }

    public void unblock() {
       Util.lock(lock);
       try {
            curr_credits_available=max_credits;
            credits_available.broadcast();
        }
        finally {
           Util.unlock(lock);
        }
    }

    // ------------------- End of management information ----------------------


    public final String getName() {
        return name;
    }

    public boolean setProperties(Properties props) {
        String  str;
        super.setProperties(props);

        str=props.getProperty("max_block_time");
        if(str != null) {
            max_block_time=Long.parseLong(str);
            props.remove("max_block_time");
        }

        str=props.getProperty("max_credits");
        if(str != null) {
            max_credits=Long.parseLong(str);
            props.remove("max_credits");
        }

        Util.checkBufferSize("SFC.max_credits", max_credits);
        MAX_CREDITS=new Long(max_credits);
        curr_credits_available=max_credits;

        str=props.getProperty("min_credit_request_interval");
        if(str != null) {
            min_credit_request_interval=Long.parseLong(str);
            props.remove("min_credit_request_interval");
        }

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }



    public void down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null && !dest.isMulticastAddress()) // only handle multicast messages
                    break;

                boolean send_credit_request=false;
                try
                {
                   lock.acquire();
                }
                catch (InterruptedException e)
                {
                   if(warn)
                      log.warn("thread was interrupted", e);
                  Thread.currentThread().interrupt(); // pass the exception on to the  caller
                  return;
                }
                try {
                    while(curr_credits_available <=0 && running) {
                        if(trace)
                            log.trace("blocking (current credits=" + curr_credits_available + ")");
                        try {
                            num_blockings++;
                            // will be signalled when we have credit responses from all members
                            credits_available.timedwait(max_block_time);
                            if(curr_credits_available <=0 && running)
                            {
                                if (trace)
                                   log.trace("Returned from timedwait but " +
                                        "credits unavailable (curr_credits_available=" +curr_credits_available +")");
                                if (min_credit_request_interval > 0) {
                                   long now = System.currentTimeMillis();
                                   if (now - last_blocked_request > min_credit_request_interval) {
                                      last_blocked_request = now;
                                      sendCreditRequest(true);
                                   }
                                }
                                else {
                                   sendCreditRequest(true);
                                }
                            }
                            else {
                               // reset the last_blocked_request stamp so the
                               // next timed out block will for sure send a request
                               last_blocked_request = 0;
                            }
                        }
                        catch(InterruptedException e) {
                            if(warn)
                                log.warn("thread was interrupted", e);
                            Thread.currentThread().interrupt(); // pass the exception on to the  caller
                            return;
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
                    Util.unlock(lock);
                }

                // we don't need to protect send_credit_request because a thread above either (a) decrements the credits
                // by the msg length and sets send_credit_request to true or (b) blocks because there are no credits
                // available. So only 1 thread can ever set send_credit_request at any given time
                if(send_credit_request) {
                    start=System.nanoTime(); // only 1 thread is here at any given time
                    passDown(evt);       // send the message before the credit request
                    sendCreditRequest(); // do this outside of the lock
                    return;
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;
        }

        passDown(evt);
    }



    public void up(Event evt) {
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
                    return; // we don't pass the request further up
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
        
        passUp(evt);
    }




    public void start() throws Exception {
        super.start();
        running=true;
    }


    public void stop() {
        super.stop();
        running=false;
        Util.lock(lock);
        try {
            credits_available.broadcast();
        }
        finally {
            Util.unlock(lock);
        }
    }


    private void handleMessage(Message msg, Address sender) {
        int len=msg.getLength(); // we don't care about headers, this is faster than size()

        Long new_val;
        boolean send_credit_response=false;

        Util.lock(received_lock);
        try {
            Long credits=(Long) received.get(sender);
            if(credits == null) {
                new_val=MAX_CREDITS;
                received.put(sender, new_val);
            }
            else {
                new_val= new Long(credits.longValue() + len);
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
            Util.unlock(lock);
        }

        if(send_credit_response) // send outside of the monitor
            sendCreditResponse(sender);
    }
    

    private void handleCreditRequest(Address sender, boolean urgent) {
        boolean send_credit_response=false;

        Util.lock(received_lock);
        try {
            num_credit_requests_received++;
            Long bytes=(Long) received.get(sender);
            if(trace)
                log.trace("received credit request from " + sender + " (total received: " + bytes + " bytes");

            if(bytes == null) {
                if(log.isErrorEnabled())
                    log.error("received credit request from " + sender + ", but sender is not in received hashmap;" +
                            " adding it");
                send_credit_response=true;
            }
            else {
                if(bytes.longValue() < max_credits && !urgent) {
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
            Util.unlock(received_lock);
        }
        
        if(send_credit_response) {
            sendCreditResponse(sender);
        }
    }

    private void handleCreditResponse(Address sender) {
        Util.lock(lock);
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
                credits_available.broadcast();
            }
        }
        finally{
            Util.unlock(lock);
        }
    }



    private void handleViewChange(View view) {
        List mbrs=view != null? view.getMembers() : null;
        if(mbrs != null) {
            synchronized(members) {
                members.clear();
                members.addAll(mbrs);
            }
        }

        Util.lock(lock);
        try {
            // remove all members which left from pending_creditors
            if(pending_creditors.retainAll(members) && pending_creditors.isEmpty()) {
                // the collection was changed and is empty now as a result of retainAll()
                curr_credits_available=max_credits;
                if(trace)
                    log.trace("replenished credits to " + curr_credits_available);
                credits_available.broadcast();
            }
        }
        finally {
            Util.unlock(lock);
        }

        Util.lock(received_lock);
        try {
            // remove left members
            received.keySet().retainAll(members);

            // add new members with *full* credits (see doc/design/SimpleFlowControl.txt for reason)
            for(Iterator it = members.iterator(); it.hasNext(); ) {
               Address mbr = (Address) it.next();
                if(!received.containsKey(mbr))
                    received.put(mbr, MAX_CREDITS);
            }

            // remove left members from pending credit requesters
            pending_requesters.retainAll(members);
        }
        finally{
            Util.unlock(received_lock);
        }
    }


    private void handleSuspect(Address suspected_mbr) {
        // this is the same as a credit response - we cannot block forever for a crashed member
        handleCreditResponse(suspected_mbr);
    }

    private void sendCreditRequest() {
        sendCreditRequest(false);
    }

    private void sendCreditRequest(boolean urgent) {
        Message credit_req=new Message();
        // credit_req.setFlag(Message.OOB); // we need to receive the credit request after regular messages
        byte type=urgent? Header.URGENT_CREDIT_REQUEST : Header.CREDIT_REQUEST;
        credit_req.putHeader(name, new Header(type));
        if(trace)
           log.trace("sending credit request to group");
        num_credit_requests_sent++;
        down_prot.down(new Event(Event.MSG, credit_req));
    }

    private void sendCreditResponse(Address dest) {
        Message credit_rsp=new Message(dest);
//        credit_rsp.setFlag(Message.OOB);
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
        public static final byte URGENT_CREDIT_REQUEST = 3;

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
                case URGENT_CREDIT_REQUEST: return "URGENT_CREDIT_REQUEST";
                default: return "<invalid type>";
            }
        }
    }


}
