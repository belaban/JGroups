
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Catches SUSPECT events traveling up the stack. Verifies that the suspected member is really dead. If yes,
 * passes SUSPECT event up the stack, otherwise discards it. Has to be placed somewhere above the FD layer and
 * below the GMS layer (receiver of the SUSPECT event). Note that SUSPECT events may be reordered by this protocol.
 * @author Bela Ban
 * @version $Id: VERIFY_SUSPECT.java,v 1.32.2.2 2008/09/03 15:13:37 vlada Exp $
 */
public class VERIFY_SUSPECT extends Protocol implements Runnable {
    private Address                local_addr=null;
    private                        long timeout=2000;   // number of millisecs to wait for an are-you-dead msg
    private                        int num_msgs=1;     // number of are-you-alive msgs and i-am-not-dead responses (for redundancy)
    final ConcurrentMap<Address,Long>  suspects=new ConcurrentHashMap<Address,Long>();  // keys=Addresses, vals=time in mcses since added
    private Thread                 timer=null;
    private boolean                use_icmp=false;     // use InetAddress.isReachable() to double-check (rather than an are-you-alive msg)
    private InetAddress            bind_addr;          // interface for ICMP pings
    /** network interface to be used to send the ICMP packets */
    private NetworkInterface       intf=null;
    static final String            name="VERIFY_SUSPECT";
    protected boolean              shutting_down=false;


    public String getName() {
        return name;
    }


    public boolean setProperties(Properties props) {
        super.setProperties(props);

        boolean ignore_systemprops=Util.isBindAddressPropertyIgnored();
        String str=Util.getProperty(new String[]{Global.BIND_ADDR, Global.BIND_ADDR_OLD}, props, "bind_addr",
                             ignore_systemprops, null);
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        str=props.getProperty("timeout");
        if(str != null) {
            timeout=Long.parseLong(str);
            props.remove("timeout");
        }

        str=props.getProperty("num_msgs");
        if(str != null) {
            num_msgs=Integer.parseInt(str);
            if(num_msgs <= 0) {
                if(log.isWarnEnabled())
                    log.warn("num_msgs is invalid (" + num_msgs + "): setting it to 1");
                num_msgs=1;
            }
            props.remove("num_msgs");
        }

        str=props.getProperty("use_icmp");
        if(str != null) {
            use_icmp=Boolean.valueOf(str).booleanValue();
            props.remove("use_icmp");
        }

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SHUTDOWN:
                shutting_down=true;
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

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                shutting_down=false;
                break;

            case Event.SUSPECT:  // it all starts here ...
                if(shutting_down)
                    return null;
                Address suspected_mbr=(Address)evt.getArg();
                if(suspected_mbr == null) {
                    if(log.isErrorEnabled()) log.error("suspected member is null");
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
                VerifyHeader hdr=(VerifyHeader)msg.getHeader(name);
                if(hdr == null)
                    break;
                if(shutting_down)
                    return null;
                switch(hdr.type) {
                    case VerifyHeader.ARE_YOU_DEAD:
                        if(hdr.from == null) {
                            if(log.isErrorEnabled()) log.error("ARE_YOU_DEAD: hdr.from is null");
                        }
                        else {
                            Message rsp;
                            for(int i=0; i < num_msgs; i++) {
                                rsp=new Message(hdr.from, null, null);
                                rsp.setFlag(Message.OOB);
                                rsp.putHeader(name, new VerifyHeader(VerifyHeader.I_AM_NOT_DEAD, local_addr));
                                down_prot.down(new Event(Event.MSG, rsp));
                            }
                        }
                        return null;
                    case VerifyHeader.I_AM_NOT_DEAD:
                        if(hdr.from == null) {
                            if(log.isErrorEnabled()) log.error("I_AM_NOT_DEAD: hdr.from is null");
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
     * Will be started when a suspect is added to the suspects hashtable. Continually iterates over the
     * entries and removes entries whose time have elapsed. For each removed entry, a SUSPECT event is passed
     * up the stack (because elapsed time means verification of member's liveness failed). Computes the shortest
     * time to wait (min of all timeouts) and waits(time) msecs. Will be woken up when entry is removed (in case
     * of successful verification of that member's liveness). Terminates when no entry remains in the hashtable.
     */
    public void run() {       
        long val, diff;

        while(!suspects.isEmpty()) {
            diff=0;

            List<Address> confirmed_suspects=new LinkedList<Address>();
            synchronized(suspects) {
                for(Address mbr : suspects.keySet()) {
                    val=suspects.get(mbr).longValue();                    
                    diff=System.currentTimeMillis() - val;
                    if(diff >= timeout) {  // haven't been unsuspected, pass up SUSPECT
                        if(log.isTraceEnabled())
                            log.trace("diff=" + diff + ", mbr " + mbr + " is dead (passing up SUSPECT event)");                      
                        
                        confirmed_suspects.add(mbr);
                        suspects.remove(mbr);
                        continue;
                    }
                    diff=Math.max(diff, timeout - diff);
                }
            }
            
            for(Address suspect:confirmed_suspects)
                up_prot.up(new Event(Event.SUSPECT,suspect));            

            if(diff > 0)
                Util.sleep(diff);
        }        
    }



    /* --------------------------------- Private Methods ----------------------------------- */


    /**
     * Sends ARE_YOU_DEAD message to suspected_mbr, wait for return or timeout
     */
    void verifySuspect(Address mbr) {
        Message msg;
        if(mbr == null)
            return;

        synchronized(suspects) {
            if(suspects.containsKey(mbr))
                return;
            suspects.put(mbr, new Long(System.currentTimeMillis()));
        }
        //start timer before we send out are you dead messages
        startTimer();

        // moved out of synchronized statement (bela): http://jira.jboss.com/jira/browse/JGRP-302
        if(log.isTraceEnabled())
            log.trace("verifying that " + mbr + " is dead");

        for(int i=0;i < num_msgs;i++) {
            msg=new Message(mbr, null, null);
            msg.setFlag(Message.OOB);
            msg.putHeader(name, new VerifyHeader(VerifyHeader.ARE_YOU_DEAD, local_addr));
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
                suspects.remove(suspected_mbr);
                up_prot.up(new Event(Event.SUSPECT, suspected_mbr));
            }
        }
        catch(Exception ex) {
            if(log.isErrorEnabled())
                log.error("failed pinging " + suspected_mbr, ex);
        }
    }

    void unsuspect(Address mbr) {
        if(mbr == null) return;
        boolean removed=false;
        synchronized(suspects) {
            if(suspects.containsKey(mbr)) {
                if(log.isTraceEnabled()) log.trace("member " + mbr + " is not dead !");
                suspects.remove(mbr);
                removed=true;
            }
        }
        if(removed) {
            down_prot.down(new Event(Event.UNSUSPECT, mbr));
            up_prot.up(new Event(Event.UNSUSPECT, mbr));
        }
    }

    /**
     * Removes all elements from suspects that are <em>not</em> in the new membership
     */
    void adjustSuspectedMembers(List new_mbrship) {
        synchronized(suspects) {
            for ( Object s : suspects.keySet() ) {
                if ( ! new_mbrship.contains ( s ) )
                    suspects.remove ( s );
            }
        }
    }

    private synchronized void startTimer() {
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

    public void start() throws Exception {
        super.start();
        shutting_down=false;
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





    public static class VerifyHeader extends Header implements Streamable {
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

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeShort(type);
            out.writeObject(from);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readShort();
            from=(Address)in.readObject();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(type);
            Util.writeAddress(from, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readShort();
            from=Util.readAddress(in);
        }

    }


}

