// $Id: StateTransferInfo.java,v 1.8 2006/03/16 16:51:49 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;


/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER
 * layer. The state is retrieved from 'target'. If target is null, then the state will be retrieved from the oldest
 * member (usually the coordinator).
 * @author Bela Ban
 * @version $Id: StateTransferInfo.java,v 1.8 2006/03/16 16:51:49 belaban Exp $
 */
public class StateTransferInfo {
    public Address target=null;
    public long    timeout=0;
    public byte[]  state=null;
    public String  state_id=null;


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


    public String toString() {
        StringBuffer ret=new StringBuffer();
        ret.append("target=" + target);
        if(state != null)
            ret.append(", state=" + state.length + " bytes");
        if(state_id != null)
            ret.append(", state_id=" + state_id);
        ret.append(", timeout=" + timeout);
        return ret.toString();
    }
}
