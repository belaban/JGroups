package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

/**
 * Failure detection based on simple heartbeat protocol. Every member
 * periodically multicasts a heartbeat. Every member also maintains a table of
 * all members (minus itself). When data or a heartbeat from P are received, we
 * reset the timestamp for P to the current time. Periodically, we check for
 * expired members, and suspect those.
 * 
 * @author Bela Ban
 * @version $Id: FD_ALL.java,v 1.19 2008/05/20 11:27:31 belaban Exp $
 */
@MBean(description="Failure detection based on simple heartbeat protocol")
public class FD_ALL extends Protocol {
    /** Map of addresses and timestamps of last updates */
    Map<Address,Long>          timestamps=new ConcurrentHashMap<Address,Long>();

    @Property
    @ManagedAttribute(description="Number of milliseconds after which a HEARTBEAT is sent to the cluster",writable=true)
    long                       interval=3000;

    @Property
    @ManagedAttribute(description="Number of milliseconds after which a " + 
                      "node P is suspected if neither a heartbeat nor data were received from P",writable=true)
    long                       timeout=5000;

    /** when a message is received from P, this is treated as if P sent a heartbeat */
    @Property
    boolean                    msg_counts_as_heartbeat=true;

    Address                    local_addr=null;
    final List<Address>        members=new ArrayList<Address>();

    @Property
    @ManagedAttribute(description="Shun switch",writable=true)
    boolean                    shun=true;
    
    TimeScheduler              timer=null;

    // task which multicasts HEARTBEAT message after 'interval' ms
    @GuardedBy("lock")
    private ScheduledFuture<?> heartbeat_sender_future=null;

    // task which checks for members exceeding timeout and suspects them
    @GuardedBy("lock")
    private ScheduledFuture<?> timeout_checker_future=null;

    private boolean            tasks_running=false;

    @ManagedAttribute(description="Number of heartbeats sent")
    protected int              num_heartbeats_sent;
    
    @ManagedAttribute(description="Number of heartbeats received")
    protected int              num_heartbeats_received=0;
    
    @ManagedAttribute(description="Number of suspected events received")
    protected int              num_suspect_events=0;

    final static String        name="FD_ALL";

    final BoundedList<Address> suspect_history=new BoundedList<Address>(20);
    final Map<Address,Integer> invalid_pingers=new HashMap<Address,Integer>(7);  // keys=Address, val=Integer (number of pings from suspected mbrs)

    final Lock                 lock=new ReentrantLock();





    public String getName() {return FD_ALL.name;}
    @ManagedAttribute(description="Member address")    
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute(description="Lists members of a cluster")
    public String getMembers() {return members != null? members.toString() : "null";}
    public int getHeartbeatsSent() {return num_heartbeats_sent;}
    public int getHeartbeatsReceived() {return num_heartbeats_received;}
    public int getSuspectEventsSent() {return num_suspect_events;}
    public long getTimeout() {return timeout;}
    public void setTimeout(long timeout) {this.timeout=timeout;}
    public long getInterval() {return interval;}
    public void setInterval(long interval) {this.interval=interval;}
    public boolean isShun() {return shun;}
    public void setShun(boolean flag) {this.shun=flag;}
    
    @ManagedAttribute(description="Are heartbeat tasks running")
    public boolean isRunning() {return tasks_running;}

    @ManagedOperation(description="Prints suspect history")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Address tmp: suspect_history) {
            sb.append(new Date()).append(": ").append(tmp).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Prints timestamps")
    public String printTimestamps() {
        return printTimeStamps();
    }
  

    public void resetStats() {
        num_heartbeats_sent=num_heartbeats_received=num_suspect_events=0;
        suspect_history.clear();
    }


    public void init() throws Exception {
        if(timer == null)
            throw new Exception("timer not set");
    }


    public void stop() {
        stopTasks();
    }


    public Object up(Event evt) {
        Message msg;
        Header  hdr;
        Address sender;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.MSG:
                msg=(Message)evt.getArg();
                hdr=(Header)msg.getHeader(name);
                if(msg_counts_as_heartbeat)
                    update(msg.getSrc()); // update when data is received too ? maybe a bit costly
                if(hdr == null)
                    break;  // message did not originate from FD_ALL layer, just pass up

                switch(hdr.type) {
                    case Header.HEARTBEAT:                       // heartbeat request; send heartbeat ack
                        sender=msg.getSrc();
                        if(sender.equals(local_addr))
                            break;
                        //if(log.isTraceEnabled())
                          //  log.trace(local_addr + ": received a heartbeat from " + sender);

                        // 2. Shun the sender of a HEARTBEAT message if that sender is not a member. This will cause
                        //    the sender to leave the group (and possibly rejoin it later)
                        if(shun && members != null && !members.contains(sender)) {
                            shunInvalidHeartbeatSender(sender);
                            break;
                        }

                        update(sender); // updates the heartbeat entry for 'sender'
                        num_heartbeats_received++;
                        break;          // don't pass up !

                    case Header.SUSPECT:
                        if(log.isTraceEnabled()) log.trace("[SUSPECT] suspect hdr is " + hdr);
                        down_prot.down(new Event(Event.SUSPECT, hdr.suspected_mbr));
                        up_prot.up(new Event(Event.SUSPECT, hdr.suspected_mbr));
                        break;

                    case Header.NOT_MEMBER:
                        if(shun) {
                            if(log.isDebugEnabled()) log.debug("[NOT_MEMBER] I'm being shunned; exiting");
                            up_prot.up(new Event(Event.EXIT));
                        }
                        break;
                }
                return null;

            case Event.INFO:
                Map<String,Object> map=(Map<String,Object>)evt.getArg();
                if(map != null) {
                    TimeScheduler tmp=(TimeScheduler)map.get("timer");
                    if(tmp != null)
                        timer=tmp;
                }
                break;
        }
        return up_prot.up(evt); // pass up to the layer above us
    }






    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                down_prot.down(evt);
                View v=(View)evt.getArg();
                handleViewChange(v);
                return null;
        }
        return down_prot.down(evt);
    }


    private void startTasks() {
        startHeartbeatSender();
        startTimeoutChecker();
        tasks_running=true;
        if(log.isTraceEnabled())
            log.trace("started heartbeat sender and timeout checker tasks");
    }

    private void stopTasks() {
        stopTimeoutChecker();
        stopHeartbeatSender();
        tasks_running=false;
        if(log.isTraceEnabled())
            log.trace("stopped heartbeat sender and timeout checker tasks");
    }

    private void startTimeoutChecker() {
        lock.lock();
        try {
            if(timeout_checker_future == null || timeout_checker_future.isDone()) {
                timeout_checker_future=timer.scheduleWithFixedDelay(new TimeoutChecker(), interval, interval, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void stopTimeoutChecker() {
         lock.lock();
         try {
             if(timeout_checker_future != null) {
                 timeout_checker_future.cancel(true);
                 timeout_checker_future=null;
             }
         }
         finally {
             lock.unlock();
         }
     }


    private void startHeartbeatSender() {
        lock.lock();
        try {
            if(heartbeat_sender_future == null || heartbeat_sender_future.isDone()) {
                heartbeat_sender_future=timer.scheduleWithFixedDelay(new HeartbeatSender(), interval, interval, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            lock.unlock();
        }
    }

     private void stopHeartbeatSender() {
        lock.lock();
        try {
            if(heartbeat_sender_future != null) {
                heartbeat_sender_future.cancel(true);
                heartbeat_sender_future=null;
            }
        }
        finally {
            lock.unlock();
        }
    }








    private void update(Address sender) {
        if(sender != null && !sender.equals(local_addr))
            timestamps.put(sender, Long.valueOf(System.currentTimeMillis()));
    }


    private void handleViewChange(View v) {
        Vector<Address> mbrs=v.getMembers();
        members.clear();
        members.addAll(mbrs);

        Set<Address> keys=timestamps.keySet();
        keys.retainAll(mbrs); // remove all nodes which have left the cluster
        for(Iterator<Address> it=mbrs.iterator(); it.hasNext();) { // and add new members
            Address mbr=it.next();
            if(mbr.equals(local_addr))
                continue;
            if(!timestamps.containsKey(mbr)) {
                timestamps.put(mbr, Long.valueOf(System.currentTimeMillis()));
            }
        }

        invalid_pingers.clear();

        if(!tasks_running && members.size() > 1)
            startTasks();
        else if(tasks_running && members.size() < 2)
            stopTasks();
    }


    /**
     * If sender is not a member, send a NOT_MEMBER to sender (after n pings received)
     */
    private void shunInvalidHeartbeatSender(Address sender) {
        int num_pings=0;
        Message shun_msg;

        if(invalid_pingers.containsKey(sender)) {
            num_pings=invalid_pingers.get(sender).intValue();
            if(num_pings >= 3) {
                if(log.isDebugEnabled())
                    log.debug(sender + " is not in " + members + " ! Shunning it");
                shun_msg=new Message(sender, null, null);
                shun_msg.setFlag(Message.OOB);
                shun_msg.putHeader(name, new Header(Header.NOT_MEMBER));
                down_prot.down(new Event(Event.MSG, shun_msg));
                invalid_pingers.remove(sender);
            }
            else {
                num_pings++;
                invalid_pingers.put(sender, new Integer(num_pings));
            }
        }
        else {
            num_pings++;
            invalid_pingers.put(sender, Integer.valueOf(num_pings));
        }
    }


    private String printTimeStamps() {
        StringBuilder sb=new StringBuilder();
        
        long current_time=System.currentTimeMillis();
        for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
            Entry<Address,Long> entry=it.next();
            sb.append(entry.getKey()).append(": ");
            sb.append(current_time - entry.getValue().longValue()).append(" ms old\n");
        }
        return sb.toString();
    }

    void suspect(Address mbr) {
        Message suspect_msg=new Message();
        suspect_msg.setFlag(Message.OOB);
        Header hdr=new Header(Header.SUSPECT, mbr);
        suspect_msg.putHeader(name, hdr);
        down_prot.down(new Event(Event.MSG, suspect_msg));
        num_suspect_events++;
        suspect_history.add(mbr);
    }


    public static class Header extends org.jgroups.Header implements Streamable {
        public static final byte HEARTBEAT  = 0;
        public static final byte SUSPECT    = 1;
        public static final byte NOT_MEMBER = 2;  // received as response by pinged mbr when we are not a member


        byte    type=Header.HEARTBEAT;
        Address suspected_mbr=null;


       /** used for externalization */
        public Header() {
        }

        public Header(byte type) {
            this.type=type;
        }

        public Header(byte type, Address suspect) {
            this(type);
            this.suspected_mbr=suspect;
        }


        public String toString() {
            switch(type) {
                case FD_ALL.Header.HEARTBEAT:
                    return "heartbeat";
                case FD_ALL.Header.SUSPECT:
                    return "SUSPECT (suspected_mbr=" + suspected_mbr + ")";
                case FD_ALL.Header.NOT_MEMBER:
                    return "NOT_MEMBER";
                default:
                    return "unknown type (" + type + ")";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeObject(suspected_mbr);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            suspected_mbr=(Address)in.readObject();
        }

        public int size() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Util.size(suspected_mbr);
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            Util.writeAddress(suspected_mbr, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            suspected_mbr=Util.readAddress(in);
        }

    }


    /**
     * Class which periodically multicasts a HEARTBEAT message to the cluster
     */
    class HeartbeatSender implements Runnable {

        public void run() {
            Message heartbeat=new Message(); // send to all
            heartbeat.setFlag(Message.OOB);
            Header hdr=new Header(Header.HEARTBEAT);
            heartbeat.putHeader(name, hdr);
            down_prot.down(new Event(Event.MSG, heartbeat));
            //if(log.isTraceEnabled())
              //  log.trace(local_addr + ": sent heartbeat to cluster");
            num_heartbeats_sent++;
        }
    }


    class TimeoutChecker extends HeartbeatSender {

        public void run() {                        
            
            if(log.isTraceEnabled())
                log.trace("checking for expired senders, table is:\n" + printTimeStamps());

            long current_time=System.currentTimeMillis(), diff;
            for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
                Entry<Address,Long> entry=it.next();
                Address key=entry.getKey();
                Long val=entry.getValue();
                if(val == null) {
                    it.remove();
                    continue;
                }
                diff=current_time - val.longValue();
                if(diff > timeout) {
                    if(log.isTraceEnabled())
                        log.trace("haven't received a heartbeat from " + key + " for " + diff + " ms, suspecting it");
                    suspect(key);
                }
            }
        }


    }




}
