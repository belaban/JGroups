// $Id: STATE_TRANSFER.java,v 1.2 2003/09/24 23:20:47 belaban Exp $

package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.List;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;


/**
 * New STATE_TRANSFER protocol based on PBCAST. Compared to the one in ./protocols, it doesn't
 * need a QUEUE layer above it. A state request is sent to a chosen member (coordinator if
 * null). That member makes a copy D of its current digest and asks the application for a copy of
 * its current state S. Then the member returns both S and D to the requester. The requester
 * first sets its digest to D and then returns the state to the application.
 * @author Bela Ban
 */
public class STATE_TRANSFER extends Protocol {
    Address  local_addr=null;
    Vector   members=new Vector();
    Message  m=null;
    boolean  is_server=false;
    long     timeout_get_appl_state=5000;
    long     timeout_return_state=5000;
    Vector   observers=new Vector();
    long     state_id=1;  // used to differentiate between state transfers (not currently used)
    List     state_requesters=new List(); // requesters of state (usually just 1, could be more)
    Digest   digest=null;
    HashMap  map=new HashMap(); // to store configuration information


    /** All protocol names have to be unique ! */
    public String getName() {
        return "STATE_TRANSFER";
    }


    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST_STATE));
        retval.addElement(new Integer(Event.SET_DIGEST));
        return retval;
    }


    public void init() throws Exception {
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());
    }


    public void start() throws Exception {
        passUp(new Event(Event.CONFIG, map));
    }


    public void up(Event evt) {
        Message     msg;
        StateHeader hdr;

        switch(evt.getType()) {

            case Event.BECOME_SERVER:
                is_server=true;
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Vector new_members=((View)evt.getArg()).getMembers();
                synchronized(members) {
                    members.removeAllElements();
                    if(new_members != null && new_members.size() > 0)
                        for(int k=0; k < new_members.size(); k++)
                            members.addElement(new_members.elementAt(k));
                }
                break;

            case Event.GET_DIGEST_STATE_OK:
                synchronized(state_requesters) {
                    if(digest != null) {
                        Trace.warn("STATE_TRANSFER.up()", "GET_DIGEST_STATE_OK: existing digest is not null, " +
                                "overwriting it !");
                    }

                    digest=(Digest)evt.getArg();
                    if(Trace.trace)
                        Trace.info("STATE_TRANSFER.up()", "GET_DIGEST_STATE_OK: digest is " +
                                digest + "\npassUp(GET_APPLSTATE)");
                    passUp(new Event(Event.GET_APPLSTATE));
                }
                return;

            case Event.MSG:
                msg=(Message)evt.getArg();

                if(!(msg.getHeader(getName()) instanceof StateHeader))
                    break;

                hdr=(StateHeader)msg.removeHeader(getName());
                switch(hdr.type) {
                    case StateHeader.STATE_REQ:
                        handleStateReq(hdr.sender, hdr.id);
                        break;
                    case StateHeader.STATE_RSP:
                        handleStateRsp(hdr.sender, hdr.digest, msg.getBuffer());
                        break;
                    default:
                        Trace.error("STATE_TRANSFER.up()", "type " + hdr.type + " not known in StateHeader");
                        break;
                }

                return;
        }

        passUp(evt);
    }


    public void down(Event evt) {
        byte[] state;
        Address target;
        StateTransferInfo info;
        StateHeader hdr;
        Message state_req, state_rsp;
        Address requester;


        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                Vector new_members=((View)evt.getArg()).getMembers();
                synchronized(members) {
                    members.removeAllElements();
                    if(new_members != null && new_members.size() > 0)
                        for(int k=0; k < new_members.size(); k++)
                            members.addElement(new_members.elementAt(k));
                }
                break;

                // generated by JChannel.getState(). currently, getting the state from more than 1 mbrs is not implemented
            case Event.GET_STATE:
                info=(StateTransferInfo)evt.getArg();
                if(info.type != StateTransferInfo.GET_FROM_SINGLE) {
                    Trace.warn("STATE_TRANSFER.down()", "[GET_STATE] (info=" + info + "): getting the state from " +
                            "all members is not currently supported by pbcast.STATE_TRANSFER, will use " +
                            "coordinator to fetch state instead");
                }
                if(info.target == null) {
                    target=determineCoordinator();
                }
                else {
                    target=info.target;
                    if(target.equals(local_addr)) {
                        Trace.error("STATE_TRANSFER.down()", "GET_STATE: cannot fetch state from myself !");
                        target=null;
                    }
                }
                if(target == null) {
                    if(Trace.trace) Trace.info("STATE_TRANSFER.down()", "GET_STATE: first member (no state)");
                    passUp(new Event(Event.GET_STATE_OK, null));
                }
                else {
                    state_req=new Message(target, null, null);
                    state_req.putHeader(getName(), new StateHeader(StateHeader.STATE_REQ, local_addr, state_id++, null));
                    if(Trace.trace) Trace.info("STATE_TRANSFER.down()", "GET_STATE: asking " + target + " for state");
                    passDown(new Event(Event.MSG, state_req));
                }
                return;                 // don't pass down any further !

            case Event.GET_APPLSTATE_OK:
                state=(byte[])evt.getArg();
                synchronized(state_requesters) {
                    if(state_requesters.size() == 0) {
                        Trace.warn("STATE_TRANSFER.down()", "GET_APPLSTATE_OK: received application state, " +
                                "but there are no requesters !");
                        return;
                    }
                    if(digest == null)
                        Trace.warn("STATE_TRANSFER.down()", "GET_APPLSTATE_OK: received application state, " +
                                "but there is no digest !");
                    else
                        digest=digest.copy();
                    for(Enumeration e=state_requesters.elements(); e.hasMoreElements();) {
                        requester=(Address)e.nextElement();
                        state_rsp=new Message(requester, null, state); // put the state into state_rsp.buffer
                        hdr=new StateHeader(StateHeader.STATE_RSP, local_addr, 0, digest);
                        state_rsp.putHeader(getName(), hdr);
                        passDown(new Event(Event.MSG, state_rsp));
                    }
                    digest=null;
                    state_requesters.removeAll();
                }
                return;                 // don't pass down any further !
        }

        passDown(evt);              // pass on to the layer below us
    }


    public boolean setProperties(Properties props) {
        String str;

        // Milliseconds to wait for application to provide requested state, events are
        // STATE_TRANSFER up and STATE_TRANSFER_OK down
        str=props.getProperty("timeout_get_appl_state");
        if(str != null) {
            timeout_get_appl_state=new Long(str).longValue();
            props.remove("timeout_get_appl_state");
        }

        // Milliseconds to wait for 1 or all members to return its/their state. 0 means wait
        // forever. States are retrieved using GroupRequest/RequestCorrelator
        str=props.getProperty("timeout_return_state");
        if(str != null) {
            timeout_return_state=new Long(str).longValue();
            props.remove("timeout_return_state");
        }

        if(props.size() > 0) {
            System.err.println("STATE_TRANSFER.setProperties(): the following " +
                    "properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }






    /* --------------------------- Private Methods -------------------------------- */


    /** Return the first element of members which is not me. Otherwise return null. */
    Address determineCoordinator() {
        Address ret=null;
        if(members != null && members.size() > 1) {
            for(int i=0; i < members.size(); i++)
                if(!local_addr.equals(members.elementAt(i)))
                    return (Address)members.elementAt(i);
        }
        return ret;
    }


    /**
     * If a state transfer is in progress, we don't need to send a GET_APPLSTATE event to the application, but
     * instead we just add the sender to the requester list so it will receive the same state when done. If not,
     * we add the sender to the requester list and send a GET_APPLSTATE event up.
     */
    void handleStateReq(Object sender, long state_id) {
        if(sender == null) {
            Trace.error("STATE_TRANSFER.handleStateReq()", "sender is null !");
            return;
        }

        synchronized(state_requesters) {
            if(state_requesters.size() > 0) {  // state transfer is in progress, digest was requested
                state_requesters.add(sender);
            }
            else {
                state_requesters.add(sender);
                digest=null;
                if(Trace.trace)
                    Trace.info("STATE_TRANSFER.handleStateReq()", "passing down GET_DIGEST_STATE");
                passDown(new Event(Event.GET_DIGEST_STATE));
            }
        }
    }


    /** Set the digest and the send the state up to the application */
    void handleStateRsp(Object sender, Digest digest, byte[] state) {
        if(digest == null)
            Trace.warn("STATE_TRANSFER.handleStateRsp()", "digest received from " +
                    sender + " is null, skipping setting digest !");
        else
            setDigest(digest);
        if(state == null)
            Trace.warn("STATE_TRANSFER.handleStateRsp()", "state received from " +
                    sender + " is null, will return null state to application");
        passUp(new Event(Event.GET_STATE_OK, state));
    }


    /** Send down a SET_DIGEST event */
    void setDigest(Digest d) {
        passDown(new Event(Event.SET_DIGEST, d));
    }

    /* ------------------------ End of Private Methods ------------------------------ */



    /**
     * Wraps data for a state request/response. Note that for a state response the actual state will <em>not</em
     * be stored in the header itself, but in the message's buffer.
     *
     */
    public static class StateHeader extends Header {
        static final int STATE_REQ=1;
        static final int STATE_RSP=2;

        Address sender=null;   // sender of state STATE_REQ or STATE_RSP
        long id=0;          // state transfer ID (to separate multiple state transfers at the same time)
        int type=0;
        Digest digest=null;   // digest of sender (if type is STATE_RSP)


        public StateHeader() {
        } // for externalization


        public StateHeader(int type, Address sender, long id, Digest digest) {
            this.type=type;
            this.sender=sender;
            this.id=id;
            this.digest=digest;
        }

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return digest;
        }


        public boolean equals(Object o) {
            StateHeader other=null;

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
            StringBuffer sb=new StringBuffer();
            sb.append("[StateHeader: type=" + type2Str(type));
            if(sender != null) sb.append(", sender=" + sender + " id=#" + id);
            if(digest != null) sb.append(", digest=" + digest);
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
            out.writeInt(type);
            out.writeObject(digest);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            sender=(Address)in.readObject();
            id=in.readLong();
            type=in.readInt();
            digest=(Digest)in.readObject();
        }

    }


}
