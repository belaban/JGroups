package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.util.Digest;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * STATE_TRANSFER protocol based on byte array transfer. A state request is sent
 * to a chosen member (coordinator if null). That member makes a copy D of its
 * current digest and asks the application for a copy of its current state S.
 * Then the member returns both S and D to the requester. The requester first
 * sets its digest to D and then returns the state to the application.
 * 
 * @author Bela Ban
 * @version $Id: STATE_TRANSFER.java,v 1.86 2009/09/06 13:51:12 belaban Exp $
 */
@MBean(description="State transfer protocol based on byte array transfer")
@DeprecatedProperty(names= { "use_flush", "flush_timeout" })
public class STATE_TRANSFER extends Protocol {

    /* --------------------------------------------- JMX statistics --------------------------------------------- */

    private long start, stop; // to measure state transfer time   
    
    private final AtomicInteger num_state_reqs=new AtomicInteger(0);

    private final AtomicLong num_bytes_sent=new AtomicLong(0);
    
    private double avg_state_size=0;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    private Address local_addr=null;

    @GuardedBy("members")
    private final Vector<Address> members=new Vector<Address>();

    /**
     * Map<String,Set> of state requesters. Keys are state IDs, values are Sets
     * of Addresses (one for each requester)
     */
    private final Map<String,Set<Address>> state_requesters=new HashMap<String,Set<Address>>();

    /** set to true while waiting for a STATE_RSP */
    private volatile boolean waiting_for_state_response=false;

    private boolean flushProtocolInStack=false;

    public STATE_TRANSFER() {}


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

    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval=new Vector<Integer>();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.OVERWRITE_DIGEST));
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

    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                StateHeader hdr=(StateHeader)msg.getHeader(getName());
                if(hdr == null)
                    break;

                switch(hdr.type) {
                    case StateHeader.STATE_REQ:
                        handleStateReq(hdr);
                        break;
                    case StateHeader.STATE_RSP:
                        // fix for https://jira.jboss.org/jira/browse/JGRP-1013
                        if(isDigestNeeded())
                            down_prot.down(new Event(Event.CLOSE_BARRIER));
                        try {
                            handleStateRsp(hdr, msg.getBuffer());
                        }
                        finally {
                            if(isDigestNeeded())
                                down_prot.down(new Event(Event.OPEN_BARRIER));
                        }
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
                            log.error("GET_STATE: cannot fetch state from myself !");
                        target=null;
                    }
                }
                if(target == null) {
                    if(log.isDebugEnabled())
                        log.debug("GET_STATE: first member (no state)");
                    up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
                }
                else {
                    Message state_req=new Message(target, null, null);
                    state_req.putHeader(getName(), new StateHeader(StateHeader.STATE_REQ,
                                                              local_addr,
                                                              System.currentTimeMillis(),
                                                              null,
                                                              info.state_id));
                    if(log.isDebugEnabled())
                        log.debug("GET_STATE: asking " + target + " for state");

                    // suspend sending and handling of message garbage collection gossip messages,
                    // fixes bugs #943480 and #938584). Wake up when state has been received
                    if(log.isDebugEnabled())
                        log.debug("passing down a SUSPEND_STABLE event");
                    down_prot.down(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));
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

    private void requestApplicationStates(Address requester, Digest digest, boolean open_barrier) {
        Set<String> appl_ids=new HashSet<String>(state_requesters.keySet());

        List<StateTransferInfo> responses=new LinkedList<StateTransferInfo>();
        for(Iterator<String> it=appl_ids.iterator();it.hasNext();) {
            String id=it.next();
            StateTransferInfo info=new StateTransferInfo(requester, id, 0L, null);
            StateTransferInfo rsp=(StateTransferInfo)up_prot.up(new Event(Event.GET_APPLSTATE, info));
            responses.add(rsp);
        }
        if(open_barrier)
            down_prot.down(new Event(Event.OPEN_BARRIER));
        for(StateTransferInfo rsp:responses) {
            sendApplicationStateResponse(rsp, digest);
        }
    }

    private void sendApplicationStateResponse(StateTransferInfo rsp, Digest digest) {
        byte[] state=rsp.state;
        String id=rsp.state_id;
        List<Message> responses=null;

        synchronized(state_requesters) {
            if(state_requesters.isEmpty()) {
                if(log.isWarnEnabled())
                    log.warn("GET_APPLSTATE_OK: received application state, but there are no requesters !");
                return;
            }
            if(stats) {
                num_state_reqs.incrementAndGet();
                if(state != null)
                    num_bytes_sent.addAndGet(state.length);
                avg_state_size=num_bytes_sent.doubleValue() / num_state_reqs.doubleValue();
            }

            Set<Address> requesters=state_requesters.get(id);
            if(requesters == null || requesters.isEmpty()) {
                log.warn("received state for id=" + id
                         + ", but there are no requesters for this ID");
            }
            else {
                responses=new LinkedList<Message>();
                for(Iterator<Address> it=requesters.iterator();it.hasNext();) {
                    Address requester=it.next();
                    Message state_rsp=new Message(requester, null, state);
                    StateHeader hdr=new StateHeader(StateHeader.STATE_RSP,
                                                    local_addr,
                                                    0,
                                                    digest,
                                                    id);
                    state_rsp.putHeader(getName(), hdr);
                    responses.add(state_rsp);
                }
                state_requesters.remove(id);
            }
        }

        if(responses != null && !responses.isEmpty()) {
            for(Message state_rsp:responses) {
                if(log.isTraceEnabled()) {
                    int length=state != null? state.length : 0;
                    log.trace("sending state for ID=" + id
                              + " to "
                              + state_rsp.getDest()
                              + " ("
                              + length
                              + " bytes)");
                }
                down_prot.down(new Event(Event.MSG, state_rsp));

                // This has to be done in a separate thread, so we don't block on FC
                // (see http://jira.jboss.com/jira/browse/JGRP-225 for details). This will be reverted once
                // we have the threadless stack  (http://jira.jboss.com/jira/browse/JGRP-181)
                // and out-of-band messages (http://jira.jboss.com/jira/browse/JGRP-205)
                //                new Thread() {
                //                    public void run() {
                //                        down_prot.down(new Event(Event.MSG, state_rsp));
                //                    }
                //                }.start();
                // down_prot.down(new Event(Event.MSG, state_rsp));
            }
        }
    }

    /**
     * Return the first element of members which is not me. Otherwise return
     * null.
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
        Address old_coord;
        Vector<Address> new_members=v.getMembers();
        boolean send_up_null_state_rsp=false;

        synchronized(members) {
            old_coord=(!members.isEmpty()? members.firstElement() : null);
            members.clear();
            members.addAll(new_members);

            // this handles the case where a coord dies during a state transfer; prevents clients from hanging forever
            // Note this only takes a coordinator crash into account, a getState(target, timeout), where target is not
            // null is not handled ! (Usually we get the state from the coordinator)
            // http://jira.jboss.com/jira/browse/JGRP-148
            if(waiting_for_state_response && old_coord != null && !members.contains(old_coord)) {
                send_up_null_state_rsp=true;
            }
        }

        if(send_up_null_state_rsp) {
            if(log.isWarnEnabled())
                log.warn("discovered that the state provider (" + old_coord
                         + ") crashed; will return null state to application");
            StateHeader hdr=new StateHeader(StateHeader.STATE_RSP, local_addr, 0, null, null);
            handleStateRsp(hdr, null); // sends up null GET_STATE_OK
        }
    }

    /**
     * If a state transfer is in progress, we don't need to send a GET_APPLSTATE
     * event to the application, but instead we just add the sender to the
     * requester list so it will receive the same state when done. If not, we
     * add the sender to the requester list and send a GET_APPLSTATE event up.
     */
    private void handleStateReq(StateHeader hdr) {
        Address sender=hdr.sender;
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error("sender is null !");
            return;
        }

        String id=hdr.state_id; // id could be null, which means get the entire state
        synchronized(state_requesters) {
            boolean empty=state_requesters.isEmpty();
            Set<Address> requesters=state_requesters.get(id);
            if(requesters == null) {
                requesters=new HashSet<Address>();
                state_requesters.put(id, requesters);
            }
            requesters.add(sender);

            if(!isDigestNeeded()) { // state transfer is in progress, digest was already requested
                requestApplicationStates(sender, null, false);
            }
            else if(empty) {
                if(!flushProtocolInStack) {
                    down_prot.down(new Event(Event.CLOSE_BARRIER));
                }
                Digest digest=(Digest)down_prot.down(new Event(Event.GET_DIGEST));
                if(log.isDebugEnabled())
                    log.debug("digest is " + digest + ", getting application state");
                try {
                    requestApplicationStates(sender, digest, !flushProtocolInStack);
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled())
                        log.error("failed getting state from application", t);
                    if(!flushProtocolInStack) {
                        down_prot.down(new Event(Event.OPEN_BARRIER));
                    }
                }
            }
        }
    }

    /** Set the digest and the send the state up to the application */
    private void handleStateRsp(StateHeader hdr, byte[] state) {
        Digest tmp_digest=hdr.my_digest;
        boolean digest_needed=isDigestNeeded();

        waiting_for_state_response=false;
        if(digest_needed && tmp_digest != null) {
            down_prot.down(new Event(Event.OVERWRITE_DIGEST, tmp_digest)); // set the digest (e.g. in NAKACK)
        }
        stop=System.currentTimeMillis();

        // resume sending and handling of message garbage collection gossip messages, fixes bugs #943480 and #938584).
        // Wakes up a previously suspended message garbage collection protocol (e.g. STABLE)
        if(log.isDebugEnabled())
            log.debug("passing down a RESUME_STABLE event");
        down_prot.down(new Event(Event.RESUME_STABLE));

        log.debug("received state, size=" + (state == null? "0" : state.length) + " bytes. Time=" + (stop - start) + " milliseconds");
        StateTransferInfo info=new StateTransferInfo(hdr.sender, hdr.state_id, 0L, state);
        up_prot.up(new Event(Event.GET_STATE_OK, info));
    }

    /* ------------------------ End of Private Methods ------------------------------ */

    /**
     * Wraps data for a state request/response. Note that for a state response
     * the actual state will <em>not</em
     * be stored in the header itself, but in the message's buffer.
     *
     */
    public static class StateHeader extends Header implements Streamable {
        public static final byte STATE_REQ=1;
        public static final byte STATE_RSP=2;

        long id=0; // state transfer ID (to separate multiple state transfers at the same time)
        byte type=0;
        Address sender; // sender of state STATE_REQ or STATE_RSP
        Digest my_digest=null; // digest of sender (if type is STATE_RSP)
        String state_id=null; // for partial state transfer
        private static final long serialVersionUID=4457830093491204405L;

        public StateHeader() { // for externalization
        }

        public StateHeader(byte type,Address sender,long id,Digest digest) {
            this.type=type;
            this.sender=sender;
            this.id=id;
            this.my_digest=digest;
        }

        public StateHeader(byte type,Address sender,long id,Digest digest,String state_id) {
            this.type=type;
            this.sender=sender;
            this.id=id;
            this.my_digest=digest;
            this.state_id=state_id;
        }

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return my_digest;
        }

        public String getStateId() {
            return state_id;
        }

        public boolean equals(Object o) {
            StateHeader other;

            if(sender != null && o != null) {
                if(!(o instanceof StateHeader))
                    return false;
                other=(StateHeader)o;
                return sender.equals(other.sender) && id == other.id;
            }
            return false;
        }

        public int hashCode() {
            if(sender != null)
                return sender.hashCode() + (int)id;
            else
                return (int)id;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("type=").append(type2Str(type));
            if(sender != null)
                sb.append(", sender=").append(sender).append(" id=").append(id);
            if(my_digest != null)
                sb.append(", digest=").append(my_digest);
            if(state_id != null)
                sb.append(", state_id=").append(state_id);
            return sb.toString();
        }

        static String type2Str(int t) {
            switch(t) {
                case STATE_REQ:
                    return "STATE_REQ";
                case STATE_RSP:
                    return "STATE_RSP";
                default:
                    return "<unknown>";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(sender);
            out.writeLong(id);
            out.writeByte(type);
            out.writeObject(my_digest);
            if(state_id == null) {
                out.writeBoolean(false);
            }
            else {
                out.writeBoolean(true);
                out.writeUTF(state_id);
            }
        }

        public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
            sender=(Address)in.readObject();
            id=in.readLong();
            type=in.readByte();
            my_digest=(Digest)in.readObject();
            if(in.readBoolean())
                state_id=in.readUTF();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(id);
            Util.writeAddress(sender, out);
            Util.writeStreamable(my_digest, out);
            Util.writeString(state_id, out);
        }

        public void readFrom(DataInputStream in) throws IOException,
                                                IllegalAccessException,
                                                InstantiationException {
            type=in.readByte();
            id=in.readLong();
            sender=Util.readAddress(in);
            my_digest=(Digest)Util.readStreamable(Digest.class, in);
            state_id=Util.readString(in);
        }

        public int size() {
            int retval=Global.LONG_SIZE + Global.BYTE_SIZE; // id and type

            retval+=Util.size(sender);

            retval+=Global.BYTE_SIZE; // presence byte for my_digest
            if(my_digest != null)
                retval+=my_digest.serializedSize();

            retval+=Global.BYTE_SIZE; // presence byte for state_id
            if(state_id != null)
                retval+=state_id.length() + 2;
            return retval;
        }
    }
}
