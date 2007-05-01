// $Id: TcpHeader.java,v 1.5 2007/05/01 10:55:10 belaban Exp $

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

    public int size() {
        return group_addr.length() +2;
    }
}
