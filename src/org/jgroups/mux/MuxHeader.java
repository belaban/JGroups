package org.jgroups.mux;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * @author Bela Ban
 * @version $Id: MuxHeader.java,v 1.2 2006/03/12 11:49:27 belaban Exp $
 */
public class MuxHeader extends Header implements Streamable {
    String id=null;

    public MuxHeader(String id) {
        this.id=id;
    }

    public MuxHeader() {
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(id);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id=in.readUTF();
    }


    public long size() {
        long retval=Global.BYTE_SIZE; // presence byte in Util.writeString
        if(id != null)
            retval+=id.length() +2; // for UTF
        return retval;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeString(id, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        id=Util.readString(in);
    }

    public String toString() {
        return id;
    }
}
