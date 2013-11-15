package org.jgroups.protocols;


import org.jgroups.Header;

import java.io.*;



/**
 * Generic transport header, used by TP.
 * @author Bela Ban
 */
public class TpHeader extends Header {
    public String channel_name=null;

    public TpHeader() { // used for externalization
    }

    public TpHeader(String n) {
        channel_name=n;
    }

    public String toString() {
        return "[cluster_name=" + channel_name + ']';
    }

    public int size() {
        return channel_name == null? 0 : channel_name.length()+2; // +2 for writeUTF(), String.length() is quick
    }

    public void writeTo(DataOutput out) throws Exception {
        out.writeUTF(channel_name);
    }

    public void readFrom(DataInput in) throws Exception {
        channel_name=in.readUTF();
    }
}
