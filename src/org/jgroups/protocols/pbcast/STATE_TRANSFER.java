
package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.util.Digest;

import java.io.*;
import java.util.*;


/**
 * New STATE_TRANSFER protocol based on PBCAST. Compared to the one in ./protocols, it doesn't
 * need a QUEUE layer above it. A state request is sent to a chosen member (coordinator if
 * null). That member makes a copy D of its current digest and asks the application for a copy of
 * its current state S. Then the member returns both S and D to the requester. The requester
 * first sets its digest to D and then returns the state to the application.
 * @author Bela Ban
 * @version $Id: STATE_TRANSFER.java,v 1.76 2007/09/17 07:03:35 belaban Exp $
 */
public class STATE_TRANSFER extends Protocol {
    Address        local_addr=null;
    final Vector   members=new Vector();
    long           state_id=1;  // used to differentiate between state transfers (not currently used)

    // final Set      state_requesters=new HashSet(); // requesters of state (usually just 1, could be more)

    /** Map<String,Set> of state requesters. Keys are state IDs, values are Sets of Addresses (one for each requester) */
    final Map      state_requesters=new HashMap();

    /** set to true while waiting for a STATE_RSP */
    boolean        waiting_for_state_response=false;

    final Map<String,Object>  map=new HashMap<String,Object>(); // to store configuration information
    long           start, stop; // to measure state transfer time
    int            num_state_reqs=0;
    long           num_bytes_sent=0;
    double         avg_state_size=0;
    final static   String name="STATE_TRANSFER";    
    boolean        flushProtocolInStack = false;


    /** All protocol names have to be unique ! */
    public String getName() {
        return name;
    }

    public int getNumberOfStateRequests() {return num_state_reqs;}
    public long getNumberOfStateBytesSent() {return num_bytes_sent;}
    public double getAverageStateSize() {return avg_state_size;}

    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.SET_DIGEST));
        return retval;
    }
   
    public void resetStats() {
        super.resetStats();
        num_state_reqs=0;
        num_bytes_sent=0;
        avg_state_size=0;
    }


    public boolean setProperties(Properties props) {
        super.setProperties(props);             
        String str=props.getProperty("use_flush");   
        if(str != null) {
            log.warn("use_flush has been deprecated and its value will be ignored");
            props.remove("use_flush");
        } 
        str=props.getProperty("flush_timeout");   
        if(str != null) {
            log.warn("flush_timeout has been deprecated and its value will be ignored");
            props.remove("flush_timeout");
        }     
                                             
        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    public void init() throws Exception {
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());
    }


    public void start() throws Exception {
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
            StateHeader hdr=(StateHeader)msg.getHeader(name);
            if(hdr == null)
                break;

            switch(hdr.type) {
            case StateHeader.STATE_REQ:
                handleStateReq(hdr);
                break;
            case StateHeader.STATE_RSP:
                handleStateRsp(hdr, msg.getBuffer());               
                break;
            default:
                if(log.isErrorEnabled()) log.error("type " + hdr.type + " not known in StateHeader");
                break;
            }
            return null;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            handleViewChange((View)evt.getArg());
            break;
            
        case Event.CONFIG :
            Map<String,Object> config = (Map<String,Object>) evt.getArg();
            if(config != null && config.containsKey("state_transfer")){
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
                        if(log.isErrorEnabled()) log.error("GET_STATE: cannot fetch state from myself !");
                        target=null;
                    }
                }
                if(target == null) {
                    if(log.isDebugEnabled()) log.debug("GET_STATE: first member (no state)");
                    up_prot.up(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
                }
                else {                    
                    Message state_req=new Message(target, null, null);
                    state_req.putHeader(name, new StateHeader(StateHeader.STATE_REQ, local_addr, state_id++, null, info.state_id));
                    if(log.isDebugEnabled()) log.debug("GET_STATE: asking " + target + " for state");

                    // suspend sending and handling of message garbage collection gossip messages,
                    // fixes bugs #943480 and #938584). Wake up when state has been received
                    if(log.isDebugEnabled())
                        log.debug("passing down a SUSPEND_STABLE event");
                    down_prot.down(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));
                    waiting_for_state_response=true;
                    start=System.currentTimeMillis();
                    down_prot.down(new Event(Event.MSG, state_req));
                }
                return null;                 // don't pass down any further !         
                
            case Event.CONFIG :
                Map<String,Object> config = (Map<String,Object>) evt.getArg();                
                if(config != null && config.containsKey("flush_supported")){
                    flushProtocolInStack = true;                                       
                }
                break;

        }

        return down_prot.down(evt);              // pass on to the layer below us
    }









    /* --------------------------- Private Methods -------------------------------- */

    /**
	 * When FLUSH is used we do not need to pass digests between members
	 *
	 * see JGroups/doc/design/PartialStateTransfer.txt
	 * see JGroups/doc/design/FLUSH.txt
	 *
	 * @return true if use of digests is required, false otherwise
	 */
	private boolean isDigestNeeded(){
		return !flushProtocolInStack;
	}

    private void requestApplicationStates(Address requester, Digest digest, boolean open_barrier) {
        Set appl_ids=new HashSet(state_requesters.keySet());
        String id;

        List<StateTransferInfo> responses=new LinkedList<StateTransferInfo>();
        for(Iterator it=appl_ids.iterator(); it.hasNext();) {
            id=(String)it.next();
            StateTransferInfo info=new StateTransferInfo(requester, id, 0L, null);
            StateTransferInfo rsp=(StateTransferInfo)up_prot.up(new Event(Event.GET_APPLSTATE, info));
            responses.add(rsp);
        }
        if(open_barrier)
            down_prot.down(new Event(Event.OPEN_BARRIER));
        for(StateTransferInfo rsp: responses) {
            sendApplicationStateResponse(rsp, digest);
        }
    }


    private void sendApplicationStateResponse(StateTransferInfo rsp, Digest digest) {
        byte[]          state=rsp.state;
        String          id=rsp.state_id;
        List<Message>   responses=null;

        synchronized(state_requesters) {
            if(state_requesters.isEmpty()) {
                if(log.isWarnEnabled())
                    log.warn("GET_APPLSTATE_OK: received application state, but there are no requesters !");
                return;
            }
            if(stats) {
                num_state_reqs++;
                if(state != null)
                    num_bytes_sent+=state.length;
                avg_state_size=num_bytes_sent / num_state_reqs;
            }

            Set requesters=(Set)state_requesters.get(id);
            if(requesters == null || requesters.isEmpty()) {
                log.warn("received state for id=" + id + ", but there are no requesters for this ID");
            }
            else {
                Address requester;
                responses=new LinkedList<Message>();
                for(Iterator it=requesters.iterator(); it.hasNext();) {
                    requester=(Address)it.next();
                    Message state_rsp=new Message(requester, null, state);
                    StateHeader hdr=new StateHeader(StateHeader.STATE_RSP, local_addr, 0, digest, id);
                    state_rsp.putHeader(name, hdr);
                    responses.add(state_rsp);
                }
                state_requesters.remove(id);
            }
        }

        if(responses != null && !responses.isEmpty()) {
            for(Message state_rsp: responses) {
                if(log.isTraceEnabled()) {
                    int length=state != null? state.length : 0;
                    log.trace("sending state for ID=" + id + " to " + state_rsp.getDest() + " (" + length + " bytes)");
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


    /** Return the first element of members which is not me. Otherwise return null. */
    private Address determineCoordinator() {
        Address ret=null;
        synchronized(members) {
            if(members != null && members.size() > 1) {
                for(int i=0; i < members.size(); i++)
                    if(!local_addr.equals(members.elementAt(i)))
                        return (Address)members.elementAt(i);
            }
        }
        return ret;
    }


    private void handleViewChange(View v) {
        Address old_coord;
        Vector new_members=v.getMembers();
        boolean send_up_null_state_rsp=false;

        synchronized(members) {
            old_coord=(Address)(!members.isEmpty()? members.firstElement() : null);
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
                log.warn("discovered that the state provider (" + old_coord + ") crashed; will return null state to application");
            StateHeader hdr=new StateHeader(StateHeader.STATE_RSP, local_addr, 0, null, null);
            handleStateRsp(hdr, null); // sends up null GET_STATE_OK
        }
    }

    /**
     * If a state transfer is in progress, we don't need to send a GET_APPLSTATE event to the application, but
     * instead we just add the sender to the requester list so it will receive the same state when done. If not,
     * we add the sender to the requester list and send a GET_APPLSTATE event up.
     */
    private void handleStateReq(StateHeader hdr) {
        Address sender=hdr.sender;
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender is null !");
            return;
        }

        String id=hdr.state_id; // id could be null, which means get the entire state
        synchronized(state_requesters) {
            boolean empty=state_requesters.isEmpty();
            Set requesters=(Set)state_requesters.get(id);
            if(requesters == null) {
                requesters=new HashSet();
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
    void handleStateRsp(StateHeader hdr, byte[] state) {
        Address sender=hdr.sender;
        Digest tmp_digest=hdr.my_digest;
        String id=hdr.state_id;
        Address state_sender=hdr.sender;

        waiting_for_state_response=false;
        if(isDigestNeeded()){
	        if(tmp_digest == null) {
	            if(log.isWarnEnabled())
	                log.warn("digest received from " + sender + " is null, skipping setting digest !");
	        }
	        else
	            down_prot.down(new Event(Event.SET_DIGEST, tmp_digest)); // set the digest (e.g. in NAKACK)
        }
        stop=System.currentTimeMillis();

        // resume sending and handling of message garbage collection gossip messages,
        // fixes bugs #943480 and #938584). Wakes up a previously suspended message garbage
        // collection protocol (e.g. STABLE)
        if(log.isDebugEnabled())
            log.debug("passing down a RESUME_STABLE event");
        down_prot.down(new Event(Event.RESUME_STABLE));

        if(state == null) {
            if(log.isWarnEnabled())
                log.warn("state received from " + sender + " is null, will return null state to application");
        }
        else
            log.debug("received state, size=" + state.length + " bytes. Time=" + (stop-start) + " milliseconds");
        StateTransferInfo info=new StateTransferInfo(state_sender, id, 0L, state);
        up_prot.up(new Event(Event.GET_STATE_OK, info));
    }

    /* ------------------------ End of Private Methods ------------------------------ */



    /**
     * Wraps data for a state request/response. Note that for a state response the actual state will <em>not</em
     * be stored in the header itself, but in the message's buffer.
     *
     */
    public static class StateHeader extends Header implements Streamable {
        public static final byte STATE_REQ=1;
        public static final byte STATE_RSP=2;


        long    id=0;               // state transfer ID (to separate multiple state transfers at the same time)
        byte    type=0;
        Address sender;             // sender of state STATE_REQ or STATE_RSP
        Digest  my_digest=null;     // digest of sender (if type is STATE_RSP)
        String  state_id=null;      // for partial state transfer
        private static final long serialVersionUID=4457830093491204405L;


        public StateHeader() {  // for externalization
        }


        public StateHeader(byte type, Address sender, long id, Digest digest) {
            this.type=type;
            this.sender=sender;
            this.id=id;
            this.my_digest=digest;
        }

        public StateHeader(byte type, Address sender, long id, Digest digest, String state_id) {
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
            if(sender != null) sb.append(", sender=").append(sender).append(" id=").append(id);
            if(my_digest != null) sb.append(", digest=").append(my_digest);
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


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
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

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
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
                retval+=state_id.length() +2;
            return retval;
        }

    }


}
