package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Digest;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * STATE_TRANSFER protocol based on byte array transfer. A state request is sent
 * to a chosen member (coordinator if null). That member makes a copy D of its
 * current digest and asks the application for a copy of its current state S.
 * Then the member returns both S and D to the requester. The requester first
 * sets its digest to D and then returns the state to the application.
 * @author Bela Ban
 * @see STATE
 * @see STATE_SOCK
 */
@MBean(description="State transfer protocol based on byte array transfer")
public class STATE_TRANSFER extends Protocol {



    /* --------------------------------------------- JMX statistics --------------------------------------------- */

    private long start, stop; // to measure state transfer time   
    
    private final AtomicInteger num_state_reqs=new AtomicInteger(0);

    private final AtomicLong num_bytes_sent=new AtomicLong(0);
    
    private double avg_state_size=0;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    private Address local_addr=null;

    @GuardedBy("members")
    private final List<Address> members=new ArrayList<Address>();

    /**
     * Set of state requesters
     */
    private final Set<Address> state_requesters=new HashSet<Address>();

    /** set to true while waiting for a STATE_RSP */
    private volatile boolean waiting_for_state_response=false;

    private boolean flushProtocolInStack=false;

    /** Used to prevent spurious open and close barrier calls */
    @ManagedAttribute(description="whether or not the barrier is closed")
    protected AtomicBoolean barrier_closed=new AtomicBoolean(false);


    @ManagedAttribute
    public int getNumberOfStateRequests() {
        return num_state_reqs.get();
    }

    @ManagedAttribute
    public long getNumberOfStateBytesSent() {
        return num_bytes_sent.get();
    }


    @ManagedAttribute
    public double getAverageStateSize() {
        return avg_state_size;
    }

    public List<Integer> requiredDownServices() {
        List<Integer> retval=new ArrayList<Integer>();
        retval.add(Event.GET_DIGEST);
        retval.add(Event.OVERWRITE_DIGEST);
        return retval;
    }

    public void resetStats() {
        super.resetStats();
        num_state_reqs.set(0);
        num_bytes_sent.set(0);
        avg_state_size=0;
    }

    public void init() throws Exception {}

    public void start() throws Exception {
        Map<String,Object> map=new HashMap<String,Object>();
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());
        up_prot.up(new Event(Event.CONFIG, map));
    }

    public void stop() {
        super.stop();
        waiting_for_state_response=false;
    }

    @ManagedOperation(description="Closes BARRIER and suspends STABLE")
    public void closeBarrierAndSuspendStable() {
        if(!isDigestNeeded() || !barrier_closed.compareAndSet(false, true))
            return;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending down CLOSE_BARRIER and SUSPEND_STABLE");
        down_prot.down(new Event(Event.CLOSE_BARRIER));
        down_prot.down(new Event(Event.SUSPEND_STABLE));
    }

    @ManagedOperation(description="Opens BARRIER and resumes STABLE")
    public void openBarrierAndResumeStable() {
        if(!isDigestNeeded() || !barrier_closed.compareAndSet(true, false))
            return;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending down OPEN_BARRIER and RESUME_STABLE");
        down_prot.down(new Event(Event.OPEN_BARRIER));
        down_prot.down(new Event(Event.RESUME_STABLE));
    }

    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                StateHeader hdr=(StateHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break;

                switch(hdr.type) {
                    case StateHeader.STATE_REQ:
                        handleStateReq(msg.getSrc());
                        break;
                    case StateHeader.STATE_RSP:
                        closeBarrierAndSuspendStable(); // fix for https://jira.jboss.org/jira/browse/JGRP-1013
                        try {
                            handleStateRsp(hdr, msg.getBuffer());
                        }
                        catch(Throwable t) {
                            handleException(t);
                        }
                        finally {
                            openBarrierAndResumeStable();
                        }
                        break;
                    case StateHeader.STATE_EX:
                        handleException((Throwable)msg.getObject());
                        break;
                    default:
                        if(log.isErrorEnabled())
                            log.error("type " + hdr.type + " not known in StateHeader");
                        break;
                }
                return null;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.CONFIG:
                Map<String,Object> config=(Map<String,Object>)evt.getArg();
                if(config != null && config.containsKey("state_transfer")) {
                    log.error("Protocol stack cannot contain two state transfer protocols. Remove either one of them");
                }
                break;
        }
        return up_prot.up(evt);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            // generated by JChannel.getState(). currently, getting the state from more than 1 mbr is not implemented
            case Event.GET_STATE:
                Address target;
                StateTransferInfo info=(StateTransferInfo)evt.getArg();
                if(info.target == null) {
                    target=determineCoordinator();
                }
                else {
                    target=info.target;
                    if(target.equals(local_addr)) {
                        if(log.isErrorEnabled())
                            log.error(local_addr + ": cannot fetch state from myself !");
                        target=null;
                    }
                }
                if(target == null) {
                    if(log.isDebugEnabled())
                        log.debug(local_addr + ": first member (no state)");
                    up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
                }
                else {
                    Message state_req=new Message(target).putHeader(this.id, new StateHeader(StateHeader.STATE_REQ));
                    if(log.isDebugEnabled())
                        log.debug(local_addr + ": asking " + target + " for state");

                    // suspend sending and handling of message garbage collection gossip messages,
                    // fixes bugs #943480 and #938584). Wake up when state has been received
                    /*if(log.isDebugEnabled())
                        log.debug("passing down a SUSPEND_STABLE event");
                    down_prot.down(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));*/
                    waiting_for_state_response=true;
                    start=System.currentTimeMillis();
                    down_prot.down(new Event(Event.MSG, state_req));
                }
                return null; // don't pass down any further !         

            case Event.CONFIG:
                Map<String,Object> config=(Map<String,Object>)evt.getArg();
                if(config != null && config.containsKey("flush_supported")) {
                    flushProtocolInStack=true;
                }
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return down_prot.down(evt); // pass on to the layer below us
    }

    /* --------------------------- Private Methods -------------------------------- */

    /**
     * When FLUSH is used we do not need to pass digests between members
     * 
     * see JGroups/doc/design/PartialStateTransfer.txt see
     * JGroups/doc/design/FLUSH.txt
     * 
     * @return true if use of digests is required, false otherwise
     */
    private boolean isDigestNeeded() {
        return !flushProtocolInStack;
    }



    /**
     * Return the first element of members which is not me. Otherwise return null.
     */
    private Address determineCoordinator() {
        synchronized(members) {
            for(Address member:members) {
                if(!local_addr.equals(member)) {
                    return member;
                }
            }
        }
        return null;
    }

    private void handleViewChange(View v) {
        Address       old_coord;
        List<Address> new_members=v.getMembers();
        boolean       send_up_exception=false;

        synchronized(members) {
            old_coord=(!members.isEmpty()? members.get(0) : null);
            members.clear();
            members.addAll(new_members);

            // this handles the case where a coord dies during a state transfer; prevents clients from hanging forever
            // Note this only takes a coordinator crash into account, a getState(target, timeout), where target is not
            // null is not handled ! (Usually we get the state from the coordinator)
            // http://jira.jboss.com/jira/browse/JGRP-148
            if(waiting_for_state_response && old_coord != null && !members.contains(old_coord))
                send_up_exception=true;
        }

        if(send_up_exception) {
            if(log.isWarnEnabled())
                log.warn(local_addr + ": discovered that the state provider (" + old_coord + ") left");
            waiting_for_state_response=false;
            Exception ex=new EOFException("state provider " + old_coord + " left");
            up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferResult(ex)));
            openBarrierAndResumeStable();
        }

        synchronized(state_requesters) {
            boolean was_empty=state_requesters.isEmpty();
            state_requesters.removeAll(new_members);
            if(!was_empty && state_requesters.isEmpty())
                openBarrierAndResumeStable();
        }
    }

    protected void handleException(Throwable exception) {
          openBarrierAndResumeStable();
          up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferResult(exception)));
    }


    private void handleStateReq(Address requester) {
        if(requester == null)
            return;

        if(log.isDebugEnabled())
            log.debug(local_addr + ": received state request from " + requester);

        synchronized(state_requesters) {
            if(state_requesters.isEmpty())
                closeBarrierAndSuspendStable();
            state_requesters.add(requester);

            Digest digest=null;
            try {
                if(isDigestNeeded())
                    digest=(Digest)down_prot.down(new Event(Event.GET_DIGEST));
                getStateFromApplication(requester, digest);
            }
            catch(Throwable t) {
                sendException(requester, t);
            }
            finally {
                if(state_requesters.remove(requester) && state_requesters.isEmpty())
                    openBarrierAndResumeStable();
            }
        }
    }


    protected void getStateFromApplication(Address requester, Digest digest) {
        StateTransferInfo rsp=(StateTransferInfo)up_prot.up(new Event(Event.GET_APPLSTATE));
        byte[] state=rsp.state;

        if(stats) {
            num_state_reqs.incrementAndGet();
            if(state != null)
                num_bytes_sent.addAndGet(state.length);
            avg_state_size=num_bytes_sent.doubleValue() / num_state_reqs.doubleValue();
        }

        Message state_rsp=new Message(requester, state).putHeader(this.id, new StateHeader(StateHeader.STATE_RSP, digest));
        if(log.isTraceEnabled()) {
            int length=state != null? state.length : 0;
            if(log.isTraceEnabled())
                log.trace(local_addr + ": sending state to " + state_rsp.getDest() + " (size=" + Util.printBytes(length) + ")");
        }
        down_prot.down(new Event(Event.MSG, state_rsp));
    }


    protected void sendException(Address requester, Throwable exception) {
        try {
            Message ex_msg=new Message(requester, exception).putHeader(getId(), new StateHeader(StateHeader.STATE_EX));
            down(new Event(Event.MSG, ex_msg));
        }
        catch(Throwable t) {
            log.error(local_addr + ": failed sending exception " + exception.toString() + " to " + requester);
        }
    }


    /** Set the digest and the send the state up to the application */
    private void handleStateRsp(StateHeader hdr, byte[] state) {
        Digest tmp_digest=hdr.my_digest;
        boolean digest_needed=isDigestNeeded();

        waiting_for_state_response=false;
        if(digest_needed && tmp_digest != null)
            down_prot.down(new Event(Event.OVERWRITE_DIGEST, tmp_digest)); // set the digest (e.g. in NAKACK)
        stop=System.currentTimeMillis();
        log.debug(local_addr + ": received state, size=" + (state == null? "0" : Util.printBytes(state.length)) +
                    ", time=" + (stop - start) + " milliseconds");
        StateTransferResult result=new StateTransferResult(state);
        up_prot.up(new Event(Event.GET_STATE_OK, result));
    }

    /* ------------------------ End of Private Methods ------------------------------ */

    /**
     * Wraps data for a state request/response. Note that for a state response
     * the actual state will <em>not</em
     * be stored in the header itself, but in the message's buffer.
     *
     */
    public static class StateHeader extends Header {
        public static final byte STATE_REQ = 1;
        public static final byte STATE_RSP = 2;
        public static final byte STATE_EX  = 3;

        protected byte    type=0;
        protected Digest  my_digest; // digest of sender (if type is STATE_RSP)

        public StateHeader() { // for externalization
        }

        public StateHeader(byte type) {
            this.type=type;
        }

        public StateHeader(byte type, Digest digest) {
            this.type=type;
            this.my_digest=digest;
        }

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return my_digest;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("type=").append(type2Str(type));
            if(my_digest != null)
                sb.append(", digest=").append(my_digest);
            return sb.toString();
        }

        static String type2Str(int t) {
            switch(t) {
                case STATE_REQ: return "STATE_REQ";
                case STATE_RSP: return "STATE_RSP";
                case STATE_EX:  return "STATE_EX";
                default:        return "<unknown>";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeStreamable(my_digest, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            my_digest=(Digest)Util.readStreamable(Digest.class, in);
        }

        public int size() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Global.BYTE_SIZE;    // presence byte for my_digest
            if(my_digest != null)
                retval+=my_digest.serializedSize(true);
            return retval;
        }
    }
}
