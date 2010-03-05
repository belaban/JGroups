package org.jgroups.protocols;


import org.jgroups.Header;

import java.io.*;



/**
 * Generic transport header, used by TP.
 * @author Bela Ban
 * @version $Id: TpHeader.java,v 1.5 2010/03/05 13:24:02 belaban Exp $
 */
public class TpHeader extends Header {
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


    public int size() {
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
