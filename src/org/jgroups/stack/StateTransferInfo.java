// $Id: StateTransferInfo.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.stack;


import java.util.Vector;
import org.jgroups.Address;



/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER
 * layer. If type is GET_FROM_SINGLE, then the state is retrieved from 'target'. If
 * target is null, then the state will be retrieved from the oldest member (usually the
 * coordinator). If type is GET_FROM_MANY, the the state is retrieved from
 * 'targets'. If targets is null, then the state is retrieved from all members.
 * @author Bela Ban
 */
public class StateTransferInfo {
    public static final int GET_FROM_SINGLE=1;  // get the state from a single member
    public static final int GET_FROM_MANY=2;    // get the state from multiple members (possibly all)

    public Address  requester=null;
    public int      type=GET_FROM_SINGLE;
    public Address  target=null;
    public Vector   targets=null;



    public StateTransferInfo(Address requester, int type, Address target) {
	this.requester=requester; this.type=type; this.target=target;
    }

    public StateTransferInfo(int type, Address target) {
	this.type=type; this.target=target;
    }


    public StateTransferInfo(int type, Vector targets) {
	this.type=type; this.targets=targets;
    }


    public String toString() {
	StringBuffer ret=new StringBuffer();
	ret.append("type=" + (type == GET_FROM_MANY ? "GET_FROM_MANY" : "GET_FROM_SINGLE") + ", ");
	if(type == GET_FROM_MANY) ret.append("targets=" + targets);
	else ret.append("target=" + target);
	return ret.toString();
    }
}
