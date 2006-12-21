package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.*;
import org.jgroups.util.*;

import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

/**
 * Failure detection based on simple heartbeat protocol. Every member preiodically multicasts a heartbeat. Every member
 * also maintains a table of all members (minus itself). When data or a heartbeat from P are received, we reset the
 * timestamp for P to the current time. Periodically, we check for expired members, and suspect those.
 * @author Bela Ban
 * @version $Id: FD_ALL.java,v 1.1 2006/12/21 19:25:12 belaban Exp $
 */
public class FD_ALL extends Protocol {
    /** Map of addresses and timestamps of last updates */
    Map<Address,Long>          timestamps=new ConcurrentHashMap<Address,Long>();

    /** Number of milliseconds after which a HEARTBEAT is sent to the cluster */
    long                       interval=3000;

    /** Number of milliseconds after which a node P is suspected if neither a heartbeat nor data were received from P */
    long                       timeout=5000;


    Address                    local_addr=null;
    final List                 members=new ArrayList();

    boolean                    shun=true;
    TimeScheduler              timer=null;

    // task which multicasts HEARTBEAT message after 'interval' ms
    private HeartbeatSender    heartbeat_sender=null;

    // task which checks for members exceeding timeout and suspects them
    private TimeoutChecker     timeout_checker=null;

    private boolean            tasks_running=false;

    protected int              num_heartbeats=0;
    protected int              num_suspect_events=0;

    final static String        name="FD_ALL";

    BoundedList                suspect_history=new BoundedList(20);
    final Map<Address,Integer> invalid_pingers=new HashMap(7);  // keys=Address, val=Integer (number of pings from suspected mbrs)

    final Lock                 lock=new ReentrantLock();





    public String getName() {return FD_ALL.name;}
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "null";}
    public int getNumberOfHeartbeatsSent() {return num_heartbeats;}
    public int getNumSuspectEventsGenerated() {return num_suspect_events;}
    public long getTimeout() {return timeout;}
    public void setTimeout(long timeout) {this.timeout=timeout;}
    public boolean isShun() {return shun;}
    public void setShun(boolean flag) {this.shun=flag;}
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Enumeration en=suspect_history.elements(); en.hasMoreElements();) {
            sb.append(new Date()).append(": ").append(en.nextElement()).append("\n");
        }
        return sb.toString();
    }


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");
        if(str != null) {
            timeout=Long.parseLong(str);
            props.remove("timeout");
        }

        str=props.getProperty("interval");
        if(str != null) {
            interval=Long.parseLong(str);
            props.remove("interval");
        }

        str=props.getProperty("shun");
        if(str != null) {
            shun=Boolean.valueOf(str).booleanValue();
            props.remove("shun");
        }

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    public void resetStats() {
        num_heartbeats=num_suspect_events=0;
        suspect_history.removeAll();
    }


    public void init() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("timer cannot be retrieved from protocol stack");
    }


    public void stop() {
        stopTasks();
    }


    public void up(Event evt) {
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
                // update(msg.getSrc()); // update when data is received too ? maybe a bit costly
                if(hdr == null)
                    break;  // message did not originate from FD_ALL layer, just pass up

                switch(hdr.type) {
                    case Header.HEARTBEAT:                       // heartbeat request; send heartbeat ack
                        sender=msg.getSrc();
                        if(trace)
                            log.trace("received a a heartbeat from " + sender);

                        // 2. Shun the sender of a HEARTBEAT message if that sender is not a member. This will cause
                        //    the sender to leave the group (and possibly rejoin it later)
                        if(shun && sender != null && members != null && !members.contains(sender)) {
                            shunInvalidHeartbeatSender(sender);
                            break;
                        }

                        update(sender); // updates the heartbeat entry for 'sender'
                        break;          // don't pass up !

                    case Header.SUSPECT:
                        if(trace) log.trace("[SUSPECT] suspect hdr is " + hdr);
                        passDown(new Event(Event.SUSPECT, hdr.suspected_mbr));
                        passUp(new Event(Event.SUSPECT, hdr.suspected_mbr));
                        break;

                    case Header.NOT_MEMBER:
                        if(shun) {
                            if(log.isDebugEnabled()) log.debug("[NOT_MEMBER] I'm being shunned; exiting");
                            passUp(new Event(Event.EXIT));
                        }
                        break;
                }
                return;
        }
        passUp(evt); // pass up to the layer above us
    }






    public void down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View v=(View)evt.getArg();
                handleViewChange(v);
                break;

            default:
                passDown(evt);
                break;
        }
    }


    private void startTasks() {
        startHeartbeatSender();
        startTimeoutChecker();
        tasks_running=true;
    }

    private void stopTasks() {
        stopTimeoutChecker();
        stopHeartbeatSender();
        tasks_running=false;
    }

    private void startTimeoutChecker() {
        lock.lock();
        try {
            if(timeout_checker == null) {
                timeout_checker=new TimeoutChecker();
                timer.add(timeout_checker);
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void startHeartbeatSender() {
        lock.lock();
        try {
            if(heartbeat_sender == null) {
                heartbeat_sender=new HeartbeatSender();
                timer.add(heartbeat_sender);
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void stopTimeoutChecker() {
        lock.lock();
        try {
            if(timeout_checker != null) {
                timeout_checker.cancel();
                timeout_checker=null;
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void stopHeartbeatSender() {
        lock.lock();
        try {
            if(heartbeat_sender != null) {
                heartbeat_sender.cancel();
                heartbeat_sender=null;
            }
        }
        finally {
            lock.unlock();
        }
    }





    private void update(Address sender) {
        if(sender != null)
            timestamps.put(sender, Long.valueOf(System.currentTimeMillis()));
    }


    private void handleViewChange(View v) {
        Vector mbrs=v.getMembers();
        members.clear();
        members.addAll(mbrs);

        Set keys=timestamps.keySet();
        keys.retainAll(mbrs); // remove all nodes which have left the cluster
        for(Iterator it=mbrs.iterator(); it.hasNext();) { // and add new members
            Address mbr=(Address)it.next();
            if(!timestamps.containsKey(mbr)) {
                timestamps.put(mbr, Long.valueOf(System.currentTimeMillis()));
            }
        }

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
                passDown(new Event(Event.MSG, shun_msg));
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

        public long size() {
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
    class HeartbeatSender implements TimeScheduler.CancellableTask {
        boolean started=true;

        public void cancel() {
            started=false;
        }

        public boolean cancelled() {
            return !started;
        }

        public long nextInterval() {
            return interval;
        }

        public void run() {
            Message heartbeat=new Message(); // send to all
            heartbeat.setFlag(Message.OOB);
            Header hdr=new Header(Header.HEARTBEAT);
            heartbeat.putHeader(name, hdr);
            passDown(new Event(Event.MSG, heartbeat));
            if(trace)
                log.trace("sent heartbeat to cluster");
            num_heartbeats++;
        }

        public String toString() {
            return Boolean.toString(started);
        }
    }


    class TimeoutChecker extends HeartbeatSender {

        public void run() {
            Map.Entry entry;
            Object key;
            Long val;
            long current_time=System.currentTimeMillis(), diff;
            for(Iterator it=timestamps.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=entry.getKey();
                val=(Long)entry.getValue();
                diff=current_time - val.longValue();
                if(diff > timeout) {
                    if(trace)
                        log.trace("haven't received a heartbeat from " + key + " for " + diff + " ms, suspecting it");
                    suspect((Address)key);
                }
            }
        }

        void suspect(Address mbr) {
            Message suspect_msg=new Message();
            suspect_msg.setFlag(Message.OOB);
            Header hdr=new Header(Header.SUSPECT, mbr);
            suspect_msg.putHeader(name, hdr);
            passDown(new Event(Event.MSG, suspect_msg));
            num_suspect_events++;
        }
    }




}
