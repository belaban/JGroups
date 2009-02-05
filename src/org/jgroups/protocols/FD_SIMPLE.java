// $Id: FD_SIMPLE.java,v 1.26 2009/02/05 09:27:45 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.Streamable;
import org.jgroups.util.TimeScheduler;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Simple failure detection protocol. Periodically sends a are-you-alive message to a randomly chosen member
 * (excluding itself) and waits for a response. If a response has not been received within timeout msecs, a counter
 * associated with that member will be incremented. If the counter exceeds max_missed_hbs, that member will be
 * suspected. When a message or a heartbeat are received, the counter is reset to 0.
 *
 * @author Bela Ban Aug 2002
 * @version $Revision: 1.26 $
 */
@Unsupported
public class FD_SIMPLE extends Protocol {
    Address       local_addr=null;
    TimeScheduler timer=null;

    final Lock    heartbeat_lock=new ReentrantLock();
    @GuardedBy("heartbeat_lock")
    Future<?>     heartbeat_future=null;
    HeartbeatTask task;

    @Property
    long interval=3000;            // interval in msecs between are-you-alive messages
    @Property
    long timeout=3000;             // time (in msecs) to wait for a response to are-you-alive
    final Vector<Address> members=new Vector<Address>();
    final Map<Address,Integer> counters=new HashMap<Address,Integer>();   // keys=Addresses, vals=Integer (count)
    @Property
    int max_missed_hbs=5;         // max number of missed responses until a member is suspected
    static final String name="FD_SIMPLE";


    public String getName() {
        return "FD_SIMPLE";
    }

    public void init() throws Exception {
        timer=getTransport().getTimer();
    }

    public void stop() {
        heartbeat_lock.lock();
        try {
            if(heartbeat_future != null) {
                heartbeat_future.cancel(true); // we need to interrupt the thread as it may call wait()
                heartbeat_future=null;
                task=null;
            }
        }
        finally {
            heartbeat_lock.unlock();
        }
    }


    public Object up(Event evt) {
        Message msg, rsp;
        Address sender;
        FdHeader hdr=null;
        boolean counter_reset=false;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.MSG:
                msg=(Message)evt.getArg();
                sender=msg.getSrc();
                resetCounter(sender);
                counter_reset=true;

                hdr=(FdHeader)msg.getHeader(name);
                if(hdr == null)
                    break;

                switch(hdr.type) {
                    case FdHeader.ARE_YOU_ALIVE:                    // are-you-alive request, send i-am-alive response
                        rsp=new Message(sender);
                        rsp.putHeader(name, new FdHeader(FdHeader.I_AM_ALIVE));
                        down_prot.down(new Event(Event.MSG, rsp));
                        return null; // don't pass up further

                    case FdHeader.I_AM_ALIVE:
                        if(log.isInfoEnabled()) log.info("received I_AM_ALIVE response from " + sender);
                        heartbeat_lock.lock();
                        try {
                            if(task != null)
                                task.receivedHeartbeatResponse(sender);
                        }
                        finally {
                            heartbeat_lock.unlock();
                        }
                        if(!counter_reset)
                            resetCounter(sender);
                        return null;

                    default:
                        if(log.isWarnEnabled()) log.warn("FdHeader type " + hdr.type + " not known");
                        return null;
                }
        }

        return up_prot.up(evt);  // pass up to the layer above us
    }


    public Object down(Event evt) {
        View new_view;
        Address key;

        switch(evt.getType()) {

            // Start heartbeat thread when we have more than 1 member; stop it when membership drops below 2
            case Event.VIEW_CHANGE:
                new_view=(View)evt.getArg();
                members.clear();
                members.addAll(new_view.getMembers());
                if(new_view.size() > 1) {
                    heartbeat_lock.lock();
                    try {
                        if(heartbeat_future == null || heartbeat_future.isDone()) {
                            task=new HeartbeatTask();
                            if(log.isInfoEnabled()) log.info("starting heartbeat task");
                            heartbeat_future=timer.scheduleWithFixedDelay(task, interval, interval, TimeUnit.MILLISECONDS);
                        }
                    }
                    finally {
                        heartbeat_lock.unlock();
                    }
                }
                else {
                    heartbeat_lock.lock();
                    try {
                        if(heartbeat_future != null) {
                            if(log.isInfoEnabled()) log.info("stopping heartbeat task");
                            heartbeat_future.cancel(true);
                            heartbeat_future=null;
                            task=null;
                        }
                    }
                    finally {
                        heartbeat_lock.unlock();
                    }
                }

                // remove all keys from 'counters' which are not in this new view
                synchronized(counters) {
                    for(Iterator<Address> it=counters.keySet().iterator(); it.hasNext();) {
                        key=it.next();
                        if(!members.contains(key)) {
                            if(log.isInfoEnabled()) log.info("removing " + key + " from counters");
                            it.remove();
                        }
                    }
                }
        }

        return down_prot.down(evt);
    }
    







    /* -------------------------------- Private Methods ------------------------------- */
    
    Address getHeartbeatDest() {
        Address retval=null;
        int r, size;
        Vector<Address> members_copy;

        if(members == null || members.size() < 2 || local_addr == null)
            return null;
        members_copy=(Vector)members.clone();
        members_copy.removeElement(local_addr); // don't select myself as heartbeat destination
        size=members_copy.size();
        r=((int)(Math.random() * (size + 1))) % size;
        retval=members_copy.elementAt(r);
        return retval;
    }


    int incrementCounter(Address mbr) {
        Integer cnt;
        int ret=0;

        if(mbr == null) return ret;
        synchronized(counters) {
            cnt=counters.get(mbr);
            if(cnt == null) {
                cnt=new Integer(0);
                counters.put(mbr, cnt);
            }
            else {
                ret=cnt.intValue() + 1;
                counters.put(mbr, new Integer(ret));
            }
            return ret;
        }
    }


    void resetCounter(Address mbr) {
        if(mbr == null) return;

        synchronized(counters) {
            counters.put(mbr, new Integer(0));
        }
    }


    String printCounters() {
        StringBuilder sb=new StringBuilder();
        Address key;

        synchronized(counters) {
            for(Iterator<Address> it=counters.keySet().iterator(); it.hasNext();) {
                key=it.next();
                sb.append(key).append(": ").append(counters.get(key)).append('\n');
            }
        }
        return sb.toString();
    }

    /* ----------------------------- End of Private Methods --------------------------- */






    public static class FdHeader extends Header implements Streamable {
        static final byte ARE_YOU_ALIVE=1;  // sent periodically to a random member
        static final byte I_AM_ALIVE=2;     // response to above message


        byte type=ARE_YOU_ALIVE;
        private static final long serialVersionUID=4021056597004641352L;

        public FdHeader() {
        } // used for externalization

        FdHeader(byte type) {
            this.type=type;
        }


        public String toString() {
            switch(type) {
                case ARE_YOU_ALIVE:
                    return "[FD_SIMPLE: ARE_YOU_ALIVE]";
                case I_AM_ALIVE:
                    return "[FD_SIMPLE: I_AM_ALIVE]";
                default:
                    return "[FD_SIMPLE: unknown type (" + type + ")]";
            }
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
        }

        public int size() {
            return Global.BYTE_SIZE;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
        }


    }


    class HeartbeatTask implements Runnable {
        final Promise<Address> promise=new Promise<Address>();
        Address dest=null;


        public void receivedHeartbeatResponse(Address from) {
            if(from != null && dest != null && from.equals(dest))
                promise.setResult(from);
        }

        public void run() {
            Message msg;
            int num_missed_hbs=0;

            dest=getHeartbeatDest();
            if(dest == null) {
                if(log.isWarnEnabled()) log.warn("heartbeat destination was null, will not send ARE_YOU_ALIVE message");
                return;
            }

            if(log.isInfoEnabled())
                log.info("sending ARE_YOU_ALIVE message to " + dest + ", counters are\n" + printCounters());

            promise.reset();
            msg=new Message(dest);
            msg.putHeader(name, new FdHeader(FdHeader.ARE_YOU_ALIVE));
            down_prot.down(new Event(Event.MSG, msg));

            promise.getResult(timeout);
            num_missed_hbs=incrementCounter(dest);
            if(num_missed_hbs >= max_missed_hbs) {
                if(log.isInfoEnabled())
                    log.info("missed " + num_missed_hbs + " from " + dest + ", suspecting member");
                up_prot.up(new Event(Event.SUSPECT, dest));
            }
        }
    }


}
