// $Id: TunnelHeader.java,v 1.6 2006/01/24 16:01:08 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;


public class TunnelHeader extends Header implements Streamable {
    public String channel_name=null;

    public TunnelHeader() {} // used for externalization

    public TunnelHeader(String n) {channel_name=n;}

    public long size() {
        return channel_name == null? 1 : channel_name.length() +3;
    }

    public String toString() {
        return "[TUNNEL:channel_name=" + channel_name + ']';
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(channel_name);
    }



    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        channel_name=(String)in.readObject();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeString(channel_name, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        channel_name=Util.readString(in);
    }

}
