// $Id: UdpHeader.java,v 1.6 2005/04/11 09:00:27 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Header;
import org.jgroups.util.Streamable;

import java.io.*;




public class UdpHeader extends Header implements Streamable {
    public String channel_name=null;

    public UdpHeader() {
    }  // used for externalization

    public UdpHeader(String n) {
        channel_name=n;
    }

    public String toString() {
        return "[UDP:channel_name=" + channel_name + ']';
    }


    public long size() {
        return 100;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(channel_name);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        channel_name=in.readUTF();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(channel_name);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        channel_name=in.readUTF();
    }
}
