// $Id: TunnelHeader.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.jgroups.*;




public class TunnelHeader extends Header {
    public String channel_name=null;

    public TunnelHeader() {} // used for externalization

    public TunnelHeader(String n) {channel_name=n;}

    public long Size() {
	return 100;
    }

    public String toString() {
	return "[TUNNEL:channel_name=" + channel_name + "]";
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeObject(channel_name);
    }



    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	channel_name=(String)in.readObject();
    }


}
