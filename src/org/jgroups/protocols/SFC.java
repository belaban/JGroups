package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;

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
 * @version $Id: SFC.java,v 1.2 2007/01/08 11:12:52 belaban Exp $
 */
public class SFC extends Protocol {
    static final String name="SFC";

    /** Max number of bytes to send per receiver until an ack must be received before continuing sending */
    private long max_credits=2000000;

    private Long MAX_CREDITS;

    private static final Long ZERO_CREDITS=new Long(0);

    /** Current number of credits available to send */
    @GuardedBy("lock")
    private long curr_credits_available=max_credits;

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

    private final List members=new LinkedList();


    private boolean running=true;





    public final String getName() {
        return name;
    }

    public void resetStats() {
        super.resetStats();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
    }

    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();
        return retval;
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
                lock.lock();
                try {
                    while(curr_credits_available <=0 && running) {
                        try {
                            credits_available.await(); // will be signalled when we have credit responses from all members
                        }
                        catch(InterruptedException e) {
                            Thread.currentThread().interrupt(); // pass the exception on to the  caller
                            return;
                        }
                    }

                    // when we get here, curr_credits_available is guaranteed to be > 0
                    curr_credits_available-=msg.getLength(); // we'll block on insufficient credits on the next down() call
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
                if(send_credit_request)
                    sendCreditRequest(); // do this outside of the lock
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
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
                new_val=received.put(sender, credits.longValue() + len);
            }

            // see whether we have any pending credit requests
            if(!pending_requesters.isEmpty()
                    && pending_requesters.contains(sender)
                    && new_val.longValue() >= max_credits) {
                pending_requesters.remove(sender);
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
            Long credits=received.get(sender);
            if(credits == null) {
                if(log.isErrorEnabled())
                    log.error("received credit request from " + sender + ", but sender is not in received hashmap;" +
                            " adding it");
                send_credit_response=true;
            }
            else {
                if(credits.longValue() < max_credits) {
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

        if(send_credit_response)
            sendCreditResponse(sender);
    }

    private void handleCreditResponse(Address sender) {
        lock.lock();
        try {
            if(pending_creditors.remove(sender) && pending_creditors.isEmpty()) {
                curr_credits_available=max_credits;
                credits_available.signalAll();
            }
        }
        finally{
            lock.unlock();
        }
    }

    private void sendCreditRequest() {
        Message credit_req=new Message();
        credit_req.setFlag(Message.OOB);
        credit_req.putHeader(name, new Header(Header.CREDIT_REQUEST));
        passDown(new Event(Event.MSG, credit_req));
    }

    private void sendCreditResponse(Address dest) {
        Message credit_rsp=new Message(dest);
        credit_rsp.setFlag(Message.OOB);
        Header hdr=new Header(Header.REPLENISH);
        credit_rsp.putHeader(name, hdr);
        passDown(new Event(Event.MSG, credit_rsp));
    }

    private void handleViewChange(View view) {
        List mbrs=view != null? view.getMembers() : null;
        if(mbrs != null) {
            synchronized(members) {
                members.clear();
                members.addAll(mbrs);
            }
        }

        // todo: add new members to received, remove left members from received
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
