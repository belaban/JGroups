package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Digest;
import org.jgroups.util.ProcessingQueue;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

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
public class STATE_TRANSFER extends Protocol implements ProcessingQueue.Handler<Address> {
    protected long                           start, stop; // to measure state transfer time
    protected final LongAdder                num_state_reqs=new LongAdder();
    protected final LongAdder                num_bytes_sent=new LongAdder();
    protected double                         avg_state_size=0;
    protected Address                        local_addr;
    protected volatile View                  view;
    protected final List<Address>            members=new ArrayList<>();

    /** List of members requesting state */
    protected final ProcessingQueue<Address> state_requesters=new ProcessingQueue<Address>().setHandler(this);

    /** set to true while waiting for a STATE_RSP */
    protected volatile boolean               waiting_for_state_response=false;

    protected boolean                        flushProtocolInStack=false;

    @ManagedAttribute public long   getNumberOfStateRequests()  {return num_state_reqs.sum();}
    @ManagedAttribute public long   getNumberOfStateBytesSent() {return num_bytes_sent.sum();}
    @ManagedAttribute public double getAverageStateSize()       {return avg_state_size;}

    public List<Integer> requiredDownServices() {
        return Arrays.asList(Event.GET_DIGEST, Event.OVERWRITE_DIGEST);
    }

    public void resetStats() {
        super.resetStats();
        num_state_reqs.reset();
        num_bytes_sent.reset();
        avg_state_size=0;
    }

    public void init() throws Exception {}

    public void start() throws Exception {
        Map<String,Object> map=new HashMap<>();
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
        if(!isDigestNeeded())
            return;
        log.trace("%s: sending down CLOSE_BARRIER and SUSPEND_STABLE", local_addr);
        down_prot.down(new Event(Event.CLOSE_BARRIER));
        down_prot.down(new Event(Event.SUSPEND_STABLE));
    }

    @ManagedOperation(description="Opens BARRIER and resumes STABLE")
    public void openBarrierAndResumeStable() {
        if(!isDigestNeeded())
            return;
        log.trace("%s: sending down OPEN_BARRIER and RESUME_STABLE", local_addr);
        down_prot.down(new Event(Event.OPEN_BARRIER));
        down_prot.down(new Event(Event.RESUME_STABLE));
    }

    public void openBarrier() {
        if(!isDigestNeeded())
            return;
        log.trace("%s: sending down OPEN_BARRIER", local_addr);
        down_prot.down(new Event(Event.OPEN_BARRIER));
    }

    public void resumeStable() {
        log.trace("%s: sending down RESUME_STABLE", local_addr);
        down_prot.down(new Event(Event.RESUME_STABLE));
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;

            case Event.CONFIG:
                Map<String,Object> config=evt.getArg();
                if(config != null && config.containsKey("state_transfer"))
                    log.error(Util.getMessage("ProtocolStackCannotContainTwoStateTransferProtocolsRemoveEitherOneOfThem"));
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        StateHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);

        switch(hdr.type) {
            case StateHeader.STATE_REQ:
                state_requesters.add(msg.getSrc());
                break;
            case StateHeader.STATE_RSP:
                handleStateRsp(hdr.getDigest(), msg.getSrc(), msg.getBuffer());
                break;
            case StateHeader.STATE_EX:
                closeHoleFor(msg.getSrc());
                try {
                    handleException(Util.exceptionFromBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength()));
                }
                catch(Throwable t) {
                    log.error("failed deserializaing state exception", t);
                }
                break;
            default:
                log.error("%s: type %s not known in StateHeader", local_addr, hdr.type);
                break;
        }
        return null;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;

            case Event.GET_STATE:
                Address target;
                StateTransferInfo info=evt.getArg();
                if(info.target == null) {
                    target=determineCoordinator();
                }
                else {
                    target=info.target;
                    if(target.equals(local_addr)) {
                        log.error("%s: cannot fetch state from myself", local_addr);
                        target=null;
                    }
                }
                if(target == null) {
                    log.debug("%s: first member (no state)", local_addr);
                    up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
                }
                else {
                    Message state_req=new Message(target).putHeader(this.id, new StateHeader(StateHeader.STATE_REQ))
                      .setFlag(Message.Flag.DONT_BUNDLE, Message.Flag.OOB, Message.Flag.SKIP_BARRIER);
                    log.debug("%s: asking %s for state", local_addr, target);

                    // suspend sending and handling of message garbage collection gossip messages,
                    // fixes bugs #943480 and #938584). Wake up when state has been received
                    /*if(log.isDebugEnabled())
                        log.debug("passing down a SUSPEND_STABLE event");
                    down_prot.down(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));*/
                    waiting_for_state_response=true;
                    start=System.currentTimeMillis();
                    down_prot.down(state_req);
                }
                return null; // don't pass down any further !         

            case Event.CONFIG:
                Map<String,Object> config=evt.getArg();
                if(config != null && config.containsKey("flush_supported")) {
                    flushProtocolInStack=true;
                }
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
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
    protected boolean isDigestNeeded() {
        return !flushProtocolInStack;
    }


    protected void punchHoleFor(Address member) {
        down_prot.down(new Event(Event.PUNCH_HOLE, member));
    }

    protected void closeHoleFor(Address member) {
        down_prot.down(new Event(Event.CLOSE_HOLE, member));
    }


    /**
     * Return the first element of members which is not me. Otherwise return null.
     */
    protected Address determineCoordinator() {
        synchronized(members) {
            for(Address member:members)
                if(!local_addr.equals(member))
                    return member;
        }
        return null;
    }

    protected void handleViewChange(View v) {
        Address       old_coord;
        List<Address> new_members=v.getMembers();
        boolean       send_up_exception=false;

        this.view=v;
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
            log.warn("%s: discovered that the state provider (%s) left", local_addr, old_coord);
            waiting_for_state_response=false;
            Exception ex=new EOFException("state provider " + old_coord + " left");
            up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferResult(ex)));
            openBarrierAndResumeStable();
        }

        // remove non members from list of members requesting state
        state_requesters.retainAll(new_members);
    }

    protected void handleException(Throwable exception) {
        if(isDigestNeeded())
            openBarrierAndResumeStable();
        up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferResult(exception)));
    }


    public void handle(Address state_requester) {
        handleStateReq(state_requester);
    }

    protected void handleStateReq(Address requester) {
        if(requester == null)
            return;

        log.debug("%s: received state request from %s", local_addr, requester);

        Digest digest=null;
        if(isDigestNeeded()) {
            try {
                punchHoleFor(requester);
                closeBarrierAndSuspendStable();
                digest=(Digest)down_prot.down(Event.GET_DIGEST_EVT);
            }
            catch(Throwable t) {
                sendException(requester, t);
                resumeStable();
                closeHoleFor(requester);
                return;
            }
            finally {
                openBarrier();
            }
        }

        // moved after reopening BARRIER (JGRP-1742)
        try {
            getStateFromApplication(requester, digest);
        }
        catch(Throwable t) {
            sendException(requester, t);
        }
        finally {
            if(isDigestNeeded()) {
                closeHoleFor(requester);
                resumeStable();
            }
        }
    }


    protected void getStateFromApplication(Address requester, Digest digest) {
        StateTransferInfo rsp=(StateTransferInfo)up_prot.up(new Event(Event.GET_APPLSTATE));
        byte[] state=rsp.state;

        if(stats) {
            num_state_reqs.increment();
            if(state != null)
                num_bytes_sent.add(state.length);
            avg_state_size=num_bytes_sent.doubleValue() / num_state_reqs.doubleValue();
        }

        Message state_rsp=new Message(requester, state).putHeader(this.id, new StateHeader(StateHeader.STATE_RSP, digest));
        log.trace("%s: sending state to %s (size=%s)", local_addr, state_rsp.getDest(), Util.printBytes(state != null? state.length : 0));
        down_prot.down(state_rsp);
    }


    protected void sendException(Address requester, Throwable exception) {
        try {
            Message ex_msg=new Message(requester).setBuffer(Util.exceptionToBuffer(exception))
              .putHeader(getId(), new StateHeader(StateHeader.STATE_EX));
            down(ex_msg);
        }
        catch(Throwable t) {
            log.error("%s: failed sending exception %s to %s", local_addr, exception, requester);
        }
    }


    /** Set the digest and the send the state up to the application */
    protected void handleStateRsp(final Digest digest, Address sender, byte[] state) {
        try {
            if(isDigestNeeded()) {
                punchHoleFor(sender);
                closeBarrierAndSuspendStable(); // fix for https://jira.jboss.org/jira/browse/JGRP-1013
                if(digest != null)
                    down_prot.down(new Event(Event.OVERWRITE_DIGEST, digest)); // set the digest (e.g. in NAKACK)
            }
            waiting_for_state_response=false;
            stop=System.currentTimeMillis();
            log.debug("%s: received state, size=%s, time=%d milliseconds", local_addr,
                      (state == null? "0" : Util.printBytes(state.length)), stop - start);
            StateTransferResult result=new StateTransferResult(state);
            up_prot.up(new Event(Event.GET_STATE_OK, result));
            down_prot.down(new Event(Event.GET_VIEW_FROM_COORD)); // https://issues.jboss.org/browse/JGRP-1751
        }
        catch(Throwable t) {
            handleException(t);
        }
        finally {
            if(isDigestNeeded()) {
                closeHoleFor(sender);
                openBarrierAndResumeStable();
            }
        }
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

        public short getMagicId() {return 57;}

        public Supplier<? extends Header> create() {
            return StateHeader::new;
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
            my_digest=Util.readStreamable(Digest.class, in);
        }

        public int serializedSize() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Global.BYTE_SIZE;    // presence byte for my_digest
            if(my_digest != null)
                retval+=my_digest.serializedSize(true);
            return retval;
        }
    }
}
