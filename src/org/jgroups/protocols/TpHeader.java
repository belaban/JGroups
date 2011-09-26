package org.jgroups.protocols;


import org.jgroups.Header;

import java.io.*;



/**
 * Generic transport header, used by TP.
 * @author Bela Ban
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

    public void writeTo(DataOutput out) throws Exception {
        out.writeUTF(channel_name);
    }

    public void readFrom(DataInput in) throws Exception {
        channel_name=in.readUTF();
        if(channel_name != null)
            size=channel_name.length()+2; // +2 for writeUTF()
    }
}
