
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * Catches SUSPECT events traveling up the stack. Verifies that the suspected member is really dead. If yes,
 * passes SUSPECT event up the stack, otherwise discards it. Has to be placed somewhere above the FD layer and
 * below the GMS layer (receiver of the SUSPECT event). Note that SUSPECT events may be reordered by this protocol.
 * @author Bela Ban
 */
@MBean(description="Double-checks suspicions reports")
public class VERIFY_SUSPECT extends Protocol implements Runnable {

    /* ------------------------------------------ Properties  ------------------------------------------ */
    
    @Property(description="Number of millisecs to wait for a response from a suspected member")
    protected long        timeout=2000;
    
    @Property(description="Number of verify heartbeats sent to a suspected member")
    protected int         num_msgs=1;
    
    @Property(description="Use InetAddress.isReachable() to verify suspected member instead of regular messages")
    protected boolean     use_icmp;

    @Property(description="Send the I_AM_NOT_DEAD message back as a multicast rather than as multiple unicasts " +
      "(default is false)")
    protected boolean     use_mcast_rsps;

    @LocalAddress
    @Property(description="Interface for ICMP pings. Used if use_icmp is true " +
            "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR})
    protected InetAddress bind_addr; // interface for ICMP pings
    
    /* --------------------------------------------- Fields ------------------------------------------------ */
    
    
    /** network interface to be used to send the ICMP packets */
    protected NetworkInterface        intf;
    
    protected Address                 local_addr;

    // a list of suspects, ordered by time when a SUSPECT event needs to be sent up
    protected final DelayQueue<Entry> suspects=new DelayQueue<>();

    @ManagedAttribute(description = "List of currently suspected members")
    public String getSuspects() {
        synchronized(suspects) {
            return suspects.toString();
        }
    }

    protected Thread timer;
    
    
    
    public VERIFY_SUSPECT() {       
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                View v=evt.getArg();
                adjustSuspectedMembers(v.getMembers());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.SUSPECT:  // it all starts here ...
                // todo: change to collections in 4.1
                Collection<Address> s=evt.arg() instanceof Address? Collections.singletonList(evt.arg()) : evt.arg();
                if(s == null)
                    return null;
                s.remove(local_addr); // ignoring suspect of self
                if(!use_icmp)
                    verifySuspect(s);
                else
                    s.forEach(this::verifySuspectWithICMP);
                return null;  // don't pass up; we will decide later (after verification) whether to pass it up

            case Event.CONFIG:
                if(bind_addr == null) {
                    Map<String,Object> config=evt.getArg();
                    bind_addr=(InetAddress)config.get("bind_addr");
                }
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        VerifyHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);
        switch(hdr.type) {
            case VerifyHeader.ARE_YOU_DEAD:
                if(hdr.from == null) {
                    log.error(Util.getMessage("AREYOUDEADHdrFromIsNull"));
                    return null;
                }
                Address target=use_mcast_rsps? null : hdr.from;
                for(int i=0; i < num_msgs; i++) {
                    Message rsp=new Message(target).setFlag(Message.Flag.INTERNAL)
                      .putHeader(this.id, new VerifyHeader(VerifyHeader.I_AM_NOT_DEAD, local_addr));
                    down_prot.down(rsp);
                }
                return null;
            case VerifyHeader.I_AM_NOT_DEAD:
                if(hdr.from == null) {
                    log.error(Util.getMessage("IAMNOTDEADHdrFromIsNull"));
                    return null;
                }
                unsuspect(hdr.from);
                return null;
        }
        return null;
    }

    /**
     * Removes all elements from suspects that are <em>not</em> in the new membership
     */
    protected void adjustSuspectedMembers(List<Address> new_mbrship) {
        synchronized(suspects) {
            suspects.removeIf(entry -> !new_mbrship.contains(entry.suspect));
        }
    }


    /**
     * Started when a suspected member is added to suspects. Iterates over the queue as long as there are suspects in
     * it and removes a suspect when the timeout for it has elapsed. Sends up a SUSPECT event for every removed suspect.
     * When a suspected member is un-suspected, the member is removed from the queue.
     */
    public void run() {
        while(!suspects.isEmpty() && timer != null) {
            try {
                Entry entry=suspects.poll(timeout,TimeUnit.MILLISECONDS);
                if(entry != null) {
                    List<Entry> expired=new ArrayList<>(suspects.size());
                    suspects.drainTo(expired); // let's see if we can remove more elements which have also expired
                    Collection<Address> suspect_list=new LinkedHashSet<>();
                    suspect_list.add(entry.suspect);
                    expired.forEach(e -> suspect_list.add(e.suspect));
                    log.trace("%s %s dead (passing up SUSPECT event)",
                              suspect_list, suspect_list.size() > 1? "are" : "is");
                    up_prot.up(new Event(Event.SUSPECT, suspect_list));
                }
            }
            catch(InterruptedException e) {
            }
        }
    }



    /* --------------------------------- Private Methods ----------------------------------- */


    /**
     * Sends ARE_YOU_DEAD message to suspected_mbr, wait for return or timeout
     */
    void verifySuspect(Collection<Address> mbrs) {
        if(mbrs == null || mbrs.isEmpty())
            return;
        boolean added=addSuspects(mbrs);
        if(added) {
            startTimer(); // start timer before we send out are you dead messages
            log.trace("verifying that %s %s dead", mbrs, mbrs.size() == 1? "is" : "are");
        }
        for(Address mbr: mbrs) {
            for(int i=0; i < num_msgs; i++) {
                Message msg=new Message(mbr).setFlag(Message.Flag.INTERNAL)
                  .putHeader(this.id, new VerifyHeader(VerifyHeader.ARE_YOU_DEAD, local_addr));
                down_prot.down(msg);
            }
        }
    }


    void verifySuspectWithICMP(Address suspected_mbr) {
        InetAddress host=suspected_mbr instanceof IpAddress? ((IpAddress)suspected_mbr).getIpAddress() : null;
        if(host == null)
            throw new IllegalArgumentException("suspected_mbr is not of type IpAddress - FD_ICMP only works with these");
        try {
            if(log.isTraceEnabled())
                log.trace("pinging host " + suspected_mbr + " using interface " + intf);
            long start=System.currentTimeMillis(), stop;
            boolean rc=host.isReachable(intf, 0, (int)timeout);
            stop=System.currentTimeMillis();
            if(rc) // success
                log.trace("successfully received response from " + host + " (after " + (stop-start) + "ms)");
            else { // failure
                log.debug("failed pinging " + suspected_mbr + " after " + (stop-start) + "ms; passing up SUSPECT event");
                removeSuspect(suspected_mbr);
                up_prot.up(new Event(Event.SUSPECT, Collections.singletonList(suspected_mbr)));
            }
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedPinging"),suspected_mbr, ex);
        }
    }

    protected boolean addSuspects(Collection<Address> list) {
        if(list == null || list.isEmpty())
            return false;
        boolean added=false;
        synchronized(suspects) {
            for(Address suspected_mbr : list) {
                boolean found_dupe=suspects.stream().anyMatch(e -> e.suspect.equals(suspected_mbr));
                if(!found_dupe) {
                    suspects.add(new Entry(suspected_mbr, System.currentTimeMillis() + timeout));
                    added=true;
                }
            }
        }
        return added;
    }

    protected boolean removeSuspect(Address suspect) {
        if(suspect == null)
            return false;
        synchronized(suspects) {
            return suspects.removeIf(e -> Objects.equals(e.suspect, suspect));
        }
    }

    public void unsuspect(Address mbr) {
        boolean removed=mbr != null && removeSuspect(mbr);
        if(removed) {
            log.trace("member " + mbr + " was unsuspected");
            down_prot.down(new Event(Event.UNSUSPECT, mbr));
            up_prot.up(new Event(Event.UNSUSPECT, mbr));
        }
    }


    protected synchronized void startTimer() {
        if(timer == null || !timer.isAlive()) {            
            timer=getThreadFactory().newThread(this,"VERIFY_SUSPECT.TimerThread");
            timer.setDaemon(true);
            timer.start();
        }
    }

    public void init() throws Exception {
        super.init();
        if(bind_addr != null)
            intf=NetworkInterface.getByInetAddress(bind_addr);
    }



    public synchronized void stop() {
        Thread tmp;
        if(timer != null && timer.isAlive()) {
            tmp=timer;
            timer=null;
            tmp.interrupt();
        }
        timer=null;
    }
    /* ----------------------------- End of Private Methods -------------------------------- */

    protected static class Entry implements Delayed {
        protected final Address suspect;
        protected final long target_time;

        public Entry(Address suspect, long target_time) {
            this.suspect=suspect;
            this.target_time=target_time;
        }

        public int compareTo(Delayed o) {
            Entry other=(Entry)o;
            long my_delay=getDelay(TimeUnit.MILLISECONDS), other_delay=other.getDelay(TimeUnit.MILLISECONDS);
            return my_delay < other_delay ? -1 : my_delay > other_delay? 1 : 0;
        }

        public long getDelay(TimeUnit unit) {
            long delay=target_time - System.currentTimeMillis();
            return unit.convert(delay, TimeUnit.MILLISECONDS);
        }

        public String toString() {
            return suspect + ": " + target_time;
        }
    }



    public static class VerifyHeader extends Header {
        static final short ARE_YOU_DEAD=1;  // 'from' is sender of verify msg
        static final short I_AM_NOT_DEAD=2;  // 'from' is suspected member

        short type=ARE_YOU_DEAD;
        Address from;     // member who wants to verify that suspected_mbr is dead


        public VerifyHeader() {
        } // used for externalization

        VerifyHeader(short type) {
            this.type=type;
        }

        VerifyHeader(short type, Address from) {
            this(type);
            this.from=from;
        }

        public short getMagicId() {return 54;}

        public Supplier<? extends Header> create() {return VerifyHeader::new;}

        public String toString() {
            switch(type) {
                case ARE_YOU_DEAD:
                    return "[VERIFY_SUSPECT: ARE_YOU_DEAD]";
                case I_AM_NOT_DEAD:
                    return "[VERIFY_SUSPECT: I_AM_NOT_DEAD]";
                default:
                    return "[VERIFY_SUSPECT: unknown type (" + type + ")]";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeShort(type);
            Util.writeAddress(from, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readShort();
            from=Util.readAddress(in);
        }

        public int serializedSize() {
            return Global.SHORT_SIZE + Util.size(from);
        }
    }


}

