// $Id: FD.java,v 1.14 2004/10/08 13:59:59 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Marshaller;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;




/**
 * Failure detection based on simple heartbeat protocol. Regularly polls members for
 * liveness. Multicasts SUSPECT messages when a member is not reachable. The simple
 * algorithms works as follows: the membership is known and ordered. Each HB protocol
 * periodically sends an 'are-you-alive' message to its *neighbor*. A neighbor is the next in
 * rank in the membership list, which is recomputed upon a view change. When a response hasn't
 * been received for n milliseconds and m tries, the corresponding member is suspected (and
 * eventually excluded if faulty).<p>
 * FD starts when it detects (in a view change notification) that there are at least
 * 2 members in the group. It stops running when the membership drops below 2.<p>
 * When a message is received from the monitored neighbor member, it causes the pinger thread to
 * 'skip' sending the next are-you-alive message. Thus, traffic is reduced.<p>
 * When we receive a ping from a member that's not in the membership list, we shun it by sending it a
 * NOT_MEMBER message. That member will then leave the group (and possibly rejoin). This is only done if
 * <code>shun</code> is true.
 * @author Bela Ban
 * @version $Revision: 1.14 $
 */
public class FD extends Protocol {
    Address               ping_dest=null;
    Address               local_addr=null;
    long                  timeout=3000;  // number of millisecs to wait for an are-you-alive msg
    long                  last_ack=System.currentTimeMillis();
    int                   num_tries=0;
    int                   max_tries=2;   // number of times to send a are-you-alive msg (tot time= max_tries*timeout)
    final Vector          members=new Vector(11);
    final Hashtable       invalid_pingers=new Hashtable(7);  // keys=Address, val=Integer (number of pings from suspected mbrs)

    /** Members from which we select ping_dest. may be subset of {@link #members} */
    final Vector          pingable_mbrs=new Vector(11);

    boolean               shun=true;
    TimeScheduler         timer=null;
    Monitor               monitor=null;  // task that performs the actual monitoring for failure detection
    private final Object  monitor_mutex=new Object();

    /** Transmits SUSPECT message until view change or UNSUSPECT is received */
    final Broadcaster   bcast_task=new Broadcaster();






    public String getName() {
        return "FD";
    }


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");
        if(str != null) {
            timeout=Long.parseLong(str);
            props.remove("timeout");
        }

        str=props.getProperty("max_tries");  // before suspecting a member
        if(str != null) {
            max_tries=Integer.parseInt(str);
            props.remove("max_tries");
        }

        str=props.getProperty("shun");
        if(str != null) {
            shun=Boolean.valueOf(str).booleanValue();
            props.remove("shun");
        }

        if(props.size() > 0) {
            System.err.println("FD.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    public void init() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("FD.init(): timer cannot be retrieved from protocol stack");
    }


    public void stop() {
        stopMonitor();
    }


    Object getPingDest(Vector mbrs) {
        Object tmp, retval=null;

        if(mbrs == null || mbrs.size() < 2 || local_addr == null)
            return null;
        for(int i=0; i < mbrs.size(); i++) {
            tmp=mbrs.elementAt(i);
            if(local_addr.equals(tmp)) {
                if(i + 1 >= mbrs.size())
                    retval=mbrs.elementAt(0);
                else
                    retval=mbrs.elementAt(i + 1);
                break;
            }
        }
        return retval;
    }


    private void startMonitor() {
        synchronized(monitor_mutex) {
            if(monitor != null && monitor.started == false) {
                monitor=null;
            }
            if(monitor == null) {
                monitor=new Monitor();
                last_ack=System.currentTimeMillis();  // start from scratch
                timer.add(monitor, true);  // fixed-rate scheduling
                num_tries=0;
            }
        }
    }

    private void stopMonitor() {
        synchronized(monitor_mutex) {
            if(monitor != null) {
                monitor.stop();
                monitor=null;
            }
        }
    }


    public void up(Event evt) {
        Message msg;
        FdHeader hdr=null;
        Object sender, tmphdr;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.MSG:
                msg=(Message)evt.getArg();
                tmphdr=msg.getHeader(getName());
                if(tmphdr == null || !(tmphdr instanceof FdHeader)) {
                    if(ping_dest != null && (sender=msg.getSrc()) != null) {
                        if(ping_dest.equals(sender)) {
                            last_ack=System.currentTimeMillis();
                            if(log.isTraceEnabled())
                                log.trace("received msg from " + sender + " (counts as ack)");
                            num_tries=0;
                        }
                    }
                    break;  // message did not originate from FD layer, just pass up
                }

                hdr=(FdHeader)msg.removeHeader(getName());
                switch(hdr.type) {
                    case FdHeader.HEARTBEAT:                       // heartbeat request; send heartbeat ack
                        Address hb_sender=msg.getSrc();
                        Message hb_ack=new Message(msg.getSrc(), null, null);
                        FdHeader tmp_hdr=new FdHeader(FdHeader.HEARTBEAT_ACK);

                        // 1.  Send an ack
                        tmp_hdr.from=local_addr;
                        hb_ack.putHeader(getName(), tmp_hdr);
                        passDown(new Event(Event.MSG, hb_ack));

                        // 2. Shun the sender of a HEARTBEAT message if that sender is not a member. This will cause
                        //    the sender to leave the group (and possibly rejoin it later)
                        if(shun)
                            shunInvalidHeartbeatSender(hb_sender);
                        break;                                     // don't pass up !

                    case FdHeader.HEARTBEAT_ACK:                   // heartbeat ack
                        if(ping_dest != null && ping_dest.equals(hdr.from)) {
                            last_ack=System.currentTimeMillis();
                            num_tries=0;
                            if(log.isDebugEnabled()) log.debug("received ack from " + hdr.from);
                        }
                        else {
                            stop();
                            ping_dest=(Address)getPingDest(members);
                            if(ping_dest != null) {
                                try {
                                    startMonitor();
                                }
                                catch(Exception ex) {
                                    if(log.isWarnEnabled()) log.warn("exception when calling startMonitor(): " + ex);
                                }
                            }
                        }
                        break;

                    case FdHeader.SUSPECT:
                        if(hdr.mbrs != null) {
                            if(log.isDebugEnabled()) log.debug("[SUSPECT] suspect hdr is " + hdr);
                            for(int i=0; i < hdr.mbrs.size(); i++) {
                                Address m=(Address)hdr.mbrs.elementAt(i);
                                if(local_addr != null && m.equals(local_addr)) {
                                    if(log.isWarnEnabled())
                                        log.warn("I was suspected, but will not remove myself from membership " +
                                                "(waiting for EXIT message)");
                                }
                                else {
                                    pingable_mbrs.remove(m);
                                    ping_dest=(Address)getPingDest(pingable_mbrs);
                                }
                                passUp(new Event(Event.SUSPECT, m));
                                passDown(new Event(Event.SUSPECT, m));
                            }
                        }
                        break;

                    case FdHeader.NOT_MEMBER:
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
        View v;

        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                synchronized(this) {
                    stop();
                    v=(View)evt.getArg();
                    members.removeAllElements();
                    members.addAll(v.getMembers());
                    bcast_task.adjustSuspectedMembers(members);
                    pingable_mbrs.removeAllElements();
                    pingable_mbrs.addAll(members);
                    passDown(evt);
                    ping_dest=(Address)getPingDest(pingable_mbrs);
                    if(ping_dest != null) {
                        try {
                            startMonitor();
                        }
                        catch(Exception ex) {
                            if(log.isWarnEnabled()) log.warn("exception when calling startMonitor(): " + ex);
                        }
                    }
                }
                break;

            case Event.UNSUSPECT:
                unsuspect((Address)evt.getArg());
                passDown(evt);
                break;

            default:
                passDown(evt);
                break;
        }
    }


    void unsuspect(Address mbr) {
        bcast_task.removeSuspectedMember(mbr);
        pingable_mbrs.removeAllElements();
        pingable_mbrs.addAll(members);
        pingable_mbrs.removeAll(bcast_task.getSuspectedMembers());
        ping_dest=(Address)getPingDest(pingable_mbrs);
    }


    /**
     * If sender is not a member, send a NOT_MEMBER to sender (after n pings received)
     */
    void shunInvalidHeartbeatSender(Address hb_sender) {
        int num_pings=0;
        Message shun_msg;

        if(hb_sender != null && members != null && !members.contains(hb_sender)) {
            if(invalid_pingers.containsKey(hb_sender)) {
                num_pings=((Integer)invalid_pingers.get(hb_sender)).intValue();
                if(num_pings >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug(hb_sender + " is not in " + members + " ! Telling it to leave group");
                    shun_msg=new Message(hb_sender, null, null);
                    shun_msg.putHeader(getName(), new FdHeader(FdHeader.NOT_MEMBER));
                    passDown(new Event(Event.MSG, shun_msg));
                    invalid_pingers.remove(hb_sender);
                }
                else {
                    num_pings++;
                    invalid_pingers.put(hb_sender, new Integer(num_pings));
                }
            }
            else {
                num_pings++;
                invalid_pingers.put(hb_sender, new Integer(num_pings));
            }
        }
    }


    public static class FdHeader extends Header implements Streamable {
        static final byte HEARTBEAT=0;
        static final byte HEARTBEAT_ACK=1;
        static final byte SUSPECT=2;
        static final byte NOT_MEMBER=3;  // received as response by pinged mbr when we are not a member


        byte    type=HEARTBEAT;
        Vector  mbrs=null;
        Address from=null;  // member who detected that suspected_mbr has failed



        public FdHeader() {
        } // used for externalization

        FdHeader(byte type) {
            this.type=type;
        }


        public String toString() {
            switch(type) {
                case HEARTBEAT:
                    return "[FD: heartbeat]";
                case HEARTBEAT_ACK:
                    return "[FD: heartbeat ack]";
                case SUSPECT:
                    return "[FD: SUSPECT (suspected_mbrs=" + mbrs + ", from=" + from + ")]";
                case NOT_MEMBER:
                    return "[FD: NOT_MEMBER]";
                default:
                    return "[FD: unknown type (" + type + ")]";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            if(mbrs == null)
                out.writeBoolean(false);
            else {
                out.writeBoolean(true);
                out.writeInt(mbrs.size());
                for(Iterator it=mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    Marshaller.write(addr, out);
                }
            }
            Marshaller.write(from, out);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            boolean mbrs_not_null=in.readBoolean();
            if(mbrs_not_null) {
                int len=in.readInt();
                mbrs=new Vector(11);
                for(int i=0; i < len; i++) {
                    Address addr=(Address)Marshaller.read(in);
                    mbrs.add(addr);
                }
            }
            from=(Address)Marshaller.read(in);
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeInt(mbrs != null? mbrs.size() : 0);
            if(mbrs != null) {
                for(Iterator it=mbrs.iterator(); it.hasNext();) {
                    Address address=(Address)it.next();
                    Util.writeAddress(address, out);
                }
            }
            Util.writeAddress(from, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            int size=in.readInt();
            if(size > 0) {
                if(mbrs == null)
                    mbrs=new Vector();
                for(int i=0; i < size; i++) {
                    Address addr=Util.readAddress(in);
                    mbrs.add(addr);
                }
            }
            from=Util.readAddress(in);
        }

    }


    private class Monitor implements TimeScheduler.Task {
        boolean started=true;

        public void stop() {
            started=false;
        }


        public boolean cancelled() {
            return !started;
        }


        public long nextInterval() {
            return timeout;
        }


        public void run() {
            Message hb_req;
            long not_heard_from=0; // time in msecs we haven't heard from ping_dest

            if(ping_dest == null) {
                if(log.isWarnEnabled())
                    log.warn("ping_dest is null: members=" + members + ", pingable_mbrs=" +
                            pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }


            // 1. send heartbeat request
            hb_req=new Message(ping_dest, null, null);
            hb_req.putHeader(getName(), new FdHeader(FdHeader.HEARTBEAT));  // send heartbeat request
            if(log.isDebugEnabled())
                log.debug("sending are-you-alive msg to " + ping_dest + " (own address=" + local_addr + ')');
            passDown(new Event(Event.MSG, hb_req));

            // 2. If the time of the last heartbeat is > timeout and max_tries heartbeat messages have not been
            //    received, then broadcast a SUSPECT message. Will be handled by coordinator, which may install
            //    a new view
            not_heard_from=System.currentTimeMillis() - last_ack;
            // quick & dirty fix: increase timeout by 500msecs to allow for latency (bela June 27 2003)
            if(not_heard_from > timeout + 500) { // no heartbeat ack for more than timeout msecs
                if(num_tries >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug("[" + local_addr + "]: received no heartbeat ack from " + ping_dest +
                                " for " + (num_tries +1) + " times (" + ((num_tries+1) * timeout) +
                                " milliseconds), suspecting it");
                    // broadcast a SUSPECT message to all members - loop until
                    // unsuspect or view change is received
                    bcast_task.addSuspectedMember(ping_dest);
                    num_tries=0;
                }
                else {
                    if(log.isDebugEnabled())
                        log.debug("heartbeat missing from " + ping_dest + " (number=" + num_tries + ')');
                    num_tries++;
                }
            }
        }


        public String toString() {
            return "" + started;
        }

    }


    /**
     * Task that periodically broadcasts a list of suspected members to the group. Goal is not to lose
     * a SUSPECT message: since these are bcast unreliably, they might get dropped. The BroadcastTask makes
     * sure they are retransmitted until a view has been received which doesn't contain the suspected members
     * any longer. Then the task terminates.
     */
    private class Broadcaster {
        final Vector suspected_mbrs=new Vector(7);
        BroadcastTask task=null;
        private final Object bcast_mutex=new Object();


        Vector getSuspectedMembers() {
            return suspected_mbrs;
        }

        /**
         * Starts a new task, or - if already running - adds the argument to the running task.
         * @param suspect
         */
        private void startBroadcastTask(Address suspect) {
            synchronized(bcast_mutex) {
                if(task == null || task.cancelled()) {
                    task=new BroadcastTask((Vector)suspected_mbrs.clone());
                    task.addSuspectedMember(suspect);
                    task.run();      // run immediately the first time
                    timer.add(task); // then every timeout milliseconds, until cancelled
                    if(log.isTraceEnabled())
                        log.trace("BroadcastTask started");
                }
                else {
                    task.addSuspectedMember(suspect);
                }
            }
        }

        private void stopBroadcastTask() {
            synchronized(bcast_mutex) {
                if(task != null) {
                    task.stop();
                    task=null;
                }
            }
        }

        /** Adds a suspected member. Starts the task if not yet running */
        void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            if(!members.contains(mbr)) return;
            synchronized(suspected_mbrs) {
                if(!suspected_mbrs.contains(mbr)) {
                    suspected_mbrs.addElement(mbr);
                    startBroadcastTask(mbr);
                }
            }
        }

        void removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return;
            if(log.isDebugEnabled()) log.debug("member is " + suspected_mbr);
            synchronized(suspected_mbrs) {
                suspected_mbrs.removeElement(suspected_mbr);
                if(suspected_mbrs.size() == 0)
                    stopBroadcastTask();
            }
        }

        void removeAll() {
            synchronized(suspected_mbrs) {
                suspected_mbrs.removeAllElements();
                stopBroadcastTask();
            }
        }

        /** Removes all elements from suspected_mbrs that are <em>not</em> in the new membership */
        void adjustSuspectedMembers(Vector new_mbrship) {
            if(new_mbrship == null || new_mbrship.size() == 0) return;
            StringBuffer sb=new StringBuffer();
            synchronized(suspected_mbrs) {
                sb.append("suspected_mbrs: ").append(suspected_mbrs);
                suspected_mbrs.retainAll(new_mbrship);
                if(suspected_mbrs.size() == 0)
                    stopBroadcastTask();
                sb.append(", after adjustment: ").append(suspected_mbrs);
                log.debug(sb.toString());
            }
        }
    }


    private class BroadcastTask implements TimeScheduler.Task {
        boolean cancelled=false;
        private Vector suspected_members=null;


        public BroadcastTask(Vector suspected_members) {
            this.suspected_members=suspected_members;
        }

        public void stop() {
            cancelled=true;
            suspected_members.clear();
            if(log.isTraceEnabled())
                log.trace("BroadcastTask stopped");
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return FD.this.timeout;
        }

        public void run() {
            Message suspect_msg;
            FD.FdHeader hdr;

            synchronized(suspected_members) {
                if(suspected_members.size() == 0) {
                    stop();
                    if(log.isDebugEnabled()) log.debug("task done (no suspected members)");
                    return;
                }

                hdr=new FdHeader(FdHeader.SUSPECT);
                hdr.mbrs=(Vector)suspected_members.clone();
                hdr.from=local_addr;
            }
            suspect_msg=new Message();       // mcast SUSPECT to all members
            suspect_msg.putHeader(getName(), hdr);
            if(log.isDebugEnabled())
                log.debug("broadcasting SUSPECT message [suspected_mbrs=" + suspected_members + "] to group");
            passDown(new Event(Event.MSG, suspect_msg));
            if(log.isDebugEnabled()) log.debug("task done");
        }

        public void addSuspectedMember(Address suspect) {
            if(suspect != null && !suspected_members.contains(suspect)) {
                suspected_members.add(suspect);
            }
        }
    }

}
