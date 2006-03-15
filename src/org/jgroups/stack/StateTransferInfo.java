// $Id: StateTransferInfo.java,v 1.4 2006/03/15 13:30:58 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;

import java.util.Vector;


/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER
 * layer. If type is GET_FROM_SINGLE, then the state is retrieved from 'target'. If
 * target is null, then the state will be retrieved from the oldest member (usually the
 * coordinator). If type is GET_FROM_MANY, the the state is retrieved from
 * 'targets'. If targets is null, then the state is retrieved from all members.
 *
 * @author Bela Ban
 */
public class StateTransferInfo {
    public Address requester=null;
    public Address target=null;
    public long    timeout=0;


    public StateTransferInfo(Address target) {
        this.target=target;
    }

    public StateTransferInfo(Address target, long timeout) {
        this.target=target;
        this.timeout=timeout;
    }



    public String toString() {
        StringBuffer ret=new StringBuffer();
        ret.append("target=" + target);
        ret.append(", timeout=" + timeout);
        return ret.toString();
    }
}
