// $Id: UdpHeader.java,v 1.5 2004/10/04 20:43:31 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Header;
import org.jgroups.util.Streamable;

import java.io.*;




public class UdpHeader extends Header implements Streamable {
    public String group_addr=null;

    public UdpHeader() {
    }  // used for externalization

    public UdpHeader(String n) {
        group_addr=n;
    }

    public String toString() {
        return "[UDP:group_addr=" + group_addr + ']';
    }


    public long size() {
        return 100;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(group_addr);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group_addr=in.readUTF();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(group_addr);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        group_addr=in.readUTF();
    }
}
