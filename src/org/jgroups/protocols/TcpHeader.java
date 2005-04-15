// $Id: TcpHeader.java,v 1.4 2005/04/15 13:17:02 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Header;
import org.jgroups.util.Streamable;

import java.io.*;




public class TcpHeader extends Header implements Streamable {
    public String group_addr=null;

    public TcpHeader() {
    } // used for externalization

    public TcpHeader(String n) {
        group_addr=n;
    }

    public String toString() {
        return "[TCP:group_addr=" + group_addr + ']';
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(group_addr);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group_addr=(String)in.readObject();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(group_addr);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        group_addr=in.readUTF();
    }

    public long size() {
        return group_addr.length() +2;
    }
}
