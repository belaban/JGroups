// $Id: PingHeader.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.jgroups.*;




public class PingHeader extends Header {
    public static final int GET_MBRS_REQ=1;   // arg = null
    public static final int GET_MBRS_RSP=2;   // arg = PingRsp(local_addr, coord_addr)

    public int    type=0;
    public Object arg=null;

    public PingHeader() {} // for externalization

    public PingHeader(int type, Object arg) {
	this.type=type; this.arg=arg;
    }

    public long size() {
	return 100;
    }

    public String toString() {
	return "[PING: type=" + type2Str(type) + ", arg=" + arg + "]";
    }

    String type2Str(int t) {
	switch(t) {
	case GET_MBRS_REQ: return "GET_MBRS_REQ";
	case GET_MBRS_RSP: return "GET_MBRS_RSP";
	default:           return "<unkown type (" + t + ")>";
	}
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeInt(type);
	out.writeObject(arg);
    }
    
    

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	type=in.readInt();
	arg=in.readObject();
    }

}
