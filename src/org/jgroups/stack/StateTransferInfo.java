// $Id: StateTransferInfo.java,v 1.9 2006/04/05 05:37:50 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;


/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER
 * layer. The state is retrieved from 'target'. If target is null, then the state will be retrieved from the oldest
 * member (usually the coordinator).
 * @author Bela Ban
 * @version $Id: StateTransferInfo.java,v 1.9 2006/04/05 05:37:50 belaban Exp $
 */
public class StateTransferInfo {
    public Address target=null;
    public long    timeout=0;
    public byte[]  state=null;
    public String  state_id=null;

    /** we could transfer the state in multiple chunks, is true this is the last one */
    public boolean last_chunk=true;


    public StateTransferInfo() {
    }

    public StateTransferInfo(Address target) {
        this.target=target;
    }

    public StateTransferInfo(Address target, long timeout) {
        this.target=target;
        this.timeout=timeout;
    }

    public StateTransferInfo(Address target, String state_id, long timeout) {
        this.target=target;
        this.state_id=state_id;
        this.timeout=timeout;
    }

    public StateTransferInfo(Address target, String state_id, long timeout, byte[] state) {
        this.target=target;
        this.state=state;
        this.state_id=state_id;
        this.timeout=timeout;
    }

    public StateTransferInfo(Address target, String state_id, long timeout, byte[] state, boolean last_chunk) {
        this.target=target;
        this.state=state;
        this.state_id=state_id;
        this.timeout=timeout;
        this.last_chunk=last_chunk;
    }


    public StateTransferInfo copy() {
        return new StateTransferInfo(target, state_id, timeout, state, last_chunk);
    }


    public String toString() {
        StringBuffer ret=new StringBuffer();
        ret.append("target=" + target);
        if(state != null)
            ret.append(", state=" + state.length + " bytes");
        if(state_id != null)
            ret.append(", state_id=" + state_id);
        ret.append(", timeout=" + timeout);
        ret.append(", last_chunk=").append(last_chunk);
        return ret.toString();
    }
}
