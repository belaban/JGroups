package org.jgroups.protocols;


import org.jgroups.Header;
import org.jgroups.util.Streamable;

import java.io.*;



/**
 * Generic transport header, used by TP
 * @author Bela Ban
 * @Version $Id: TpHeader.java,v 1.2 2005/06/24 11:20:41 belaban Exp $
 */
public class TpHeader extends Header implements Streamable {
    public String channel_name=null;
    int size=0;

    public TpHeader() {
    }  // used for externalization

    public TpHeader(String n) {
        channel_name=n;
        if(channel_name != null)
            size=channel_name.length()+2; // +2 for writeUTF()
    }

    public String toString() {
        return "[channel_name=" + channel_name + ']';
    }


    public long size() {
        return size;
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
        if(channel_name != null)
            size=channel_name.length()+2; // +2 for writeUTF()
    }
}
