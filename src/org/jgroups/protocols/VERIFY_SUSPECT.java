
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


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
    protected boolean     use_icmp=false;

    @Property(description="Send the I_AM_NOT_DEAD message back as a multicast rather than as multiple unicasts " +
      "(default is false)")
    protected boolean     use_mcast_rsps=false;

    @LocalAddress
    @Property(description="Interface for ICMP pings. Used if use_icmp is true " +
            "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR})
    protected InetAddress bind_addr; // interface for ICMP pings
    
    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class, 
    		description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr")
    protected String    bind_interface_str=null;
     
    /* --------------------------------------------- Fields ------------------------------------------------ */   
    
    
    /** network interface to be used to send the ICMP packets */
    protected NetworkInterface intf=null;
    
    protected Address local_addr=null;

    // a list of suspects, ordered by time when a SUSPECT event needs to be sent up
    protected final DelayQueue<Entry> suspects=new DelayQueue<>();

    @ManagedAttribute(description = "List of currently suspected members")
    public String getSuspects() {
        synchronized (suspects) {
            return suspects.toString();
        }
    }

    protected Thread timer=null;
    
    
    
    public VERIFY_SUSPECT() {       
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                View v=(View)evt.getArg();
                adjustSuspectedMembers(v.getMembers());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.SUSPECT:  // it all starts here ...
                Address suspected_mbr=(Address)evt.getArg();
                if(suspected_mbr == null) {
                    if(log.isErrorEnabled()) log.error(Util.getMessage("SuspectedMemberIsNull"));
                    return null;
                }

                if(local_addr != null && local_addr.equals(suspected_mbr)) {
                    if(log.isTraceEnabled())
                        log.trace("I was suspected; ignoring SUSPECT message");
                    return null;
                }

                if(!use_icmp)
                    verifySuspect(suspected_mbr);
                else
                    verifySuspectWithICMP(suspected_mbr);
                return null;  // don't pass up; we will decide later (after verification) whether to pass it up


            case Event.MSG:
                Message msg=(Message)evt.getArg();
                VerifyHeader hdr=(VerifyHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                switch(hdr.type) {
                    case VerifyHeader.ARE_YOU_DEAD:
                        if(hdr.from == null) {
                            if(log.isErrorEnabled()) log.error(Util.getMessage("AREYOUDEADHdrFromIsNull"));
                        }
                        else {
                            Message rsp;
                            Address target=use_mcast_rsps? null : hdr.from;
                            for(int i=0; i < num_msgs; i++) {
                                rsp=new Message(target).setFlag(Message.Flag.INTERNAL)
                                  .putHeader(this.id, new VerifyHeader(VerifyHeader.I_AM_NOT_DEAD, local_addr));
                                down_prot.down(new Event(Event.MSG, rsp));
                            }
                        }
                        return null;
                    case VerifyHeader.I_AM_NOT_DEAD:
                        if(hdr.from == null) {
                            if(log.isErrorEnabled()) log.error(Util.getMessage("IAMNOTDEADHdrFromIsNull"));
                            return null;
                        }
                        unsuspect(hdr.from);
                        return null;
                }
                return null;


            case Event.CONFIG:
                if(bind_addr == null) {
                    Map<String,Object> config=(Map<String,Object>)evt.getArg();
                    bind_addr=(InetAddress)config.get("bind_addr");
                }
        }
        return up_prot.up(evt);
    }

    /**
     * Removes all elements from suspects that are <em>not</em> in the new membership
     */
    protected void adjustSuspectedMembers(List<Address> new_mbrship) {
        synchronized(suspects) {
            for(Iterator<Entry> it=suspects.iterator(); it.hasNext();) {
                Entry entry=it.next();
                if(!new_mbrship.contains(entry.suspect))
                    it.remove();
            }
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
                Entry entry=suspects.poll(timeout * 2,TimeUnit.MILLISECONDS);
                if(entry != null) {
                    if(log.isTraceEnabled())
                        log.trace(entry.suspect + " is dead (passing up SUSPECT event)");
                    up_prot.up(new Event(Event.SUSPECT, entry.suspect));
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
    void verifySuspect(Address mbr) {
        Message msg;
        if(mbr == null) return;

        addSuspect(mbr);

        startTimer(); // start timer before we send out are you dead messages
        
        // moved out of synchronized statement (bela): http://jira.jboss.com/jira/browse/JGRP-302
        if(log.isTraceEnabled()) log.trace("verifying that " + mbr + " is dead");
        
        for(int i=0; i < num_msgs; i++) {
            msg=new Message(mbr).setFlag(Message.Flag.INTERNAL)
              .putHeader(this.id, new VerifyHeader(VerifyHeader.ARE_YOU_DEAD, local_addr));
            down_prot.down(new Event(Event.MSG, msg));
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
            if(rc) { // success
                if(log.isTraceEnabled())
                    log.trace("successfully received response from " + host + " (after " + (stop-start) + "ms)");
            }
            else { // failure
                if(log.isTraceEnabled())
                    log.debug("could not ping " + suspected_mbr + " after " + (stop-start) + "ms; " +
                            "passing up SUSPECT event");
                removeSuspect(suspected_mbr);
                up_prot.up(new Event(Event.SUSPECT, suspected_mbr));
            }
        }
        catch(Exception ex) {
            if(log.isErrorEnabled())
                log.error(Util.getMessage("FailedPinging"),suspected_mbr, ex);
        }
    }

    protected boolean addSuspect(Address suspect) {
        if(suspect == null)
            return false;
        synchronized(suspects) {
            for(Entry entry: suspects) // check for duplicates
                if(entry.suspect.equals(suspect))
                    return false;
            suspects.add(new Entry(suspect, System.currentTimeMillis() + timeout));
            return true;
        }
    }

    protected boolean removeSuspect(Address suspect) {
        if(suspect == null)
            return false;
        boolean retval=false;
        synchronized(suspects) {
            for(Iterator<Entry> it=suspects.iterator(); it.hasNext();) {
                Entry entry=it.next();
                if(entry.suspect.equals(suspect)) {
                    it.remove();
                    retval=true; // don't break, possibly remove more (2nd line of defense)
                }
            }
        }
        return retval;
    }

    public void unsuspect(Address mbr) {
        boolean removed=mbr != null && removeSuspect(mbr);
        if(removed) {
            if(log.isTraceEnabled()) log.trace("member " + mbr + " was unsuspected");
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
            tmp=null;
        }
        timer=null;
    }
    /* ----------------------------- End of Private Methods -------------------------------- */

    protected class Entry implements Delayed {
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
        Address from=null;     // member who wants to verify that suspected_mbr is dead


        public VerifyHeader() {
        } // used for externalization

        VerifyHeader(short type) {
            this.type=type;
        }

        VerifyHeader(short type, Address from) {
            this(type);
            this.from=from;
        }


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

        public int size() {
            return Global.SHORT_SIZE + Util.size(from);
        }
    }


}

