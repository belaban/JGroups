
package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.ViewId;
import org.jgroups.util.Util;

import java.io.*;


/**
 * @author Bela Ban
 */
public class PingHeader extends Header {
    public static final byte GET_MBRS_REQ=1;   // arg = null
    public static final byte GET_MBRS_RSP=2;   // arg = PingData (local_addr, creator)

    public byte     type=0;
    public PingData data=null;
    public String   cluster_name=null;

    // when set, we don't need the address mappings, but only the view.
    // This is typically done for a merge-triggered discovery request
    public ViewId   view_id=null;

    
    public PingHeader() {
    }

    public PingHeader(byte type, String cluster_name) {
        this.type=type;
        this.cluster_name=cluster_name;
    }

    public PingHeader(byte type, PingData data) {
        this.type=type;
        this.data=data;
    }

    public PingHeader(byte type, PingData data, String cluster_name) {
        this(type, data);
        this.cluster_name=cluster_name;
    }

    public int size() {
        int retval=Global.BYTE_SIZE *3; // type, data presence and cluster_name presence
        if(data != null)
            retval+=data.size();
        if(cluster_name != null)
            retval += cluster_name.length() +2;

        retval+=Util.size(view_id);
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[PING: type=" + type2Str(type));
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        if(data != null)
            sb.append(", arg=" + data);
        if(view_id != null)
            sb.append(", view_id=").append(view_id);
        sb.append(']');
        return sb.toString();
    }

    static String type2Str(byte t) {
        switch(t) {
            case GET_MBRS_REQ: return "GET_MBRS_REQ";
            case GET_MBRS_RSP: return "GET_MBRS_RSP";
            default:           return "<unkown type (" + t + ")>";
        }
    }


    public void writeTo(DataOutput outstream) throws Exception {
        outstream.writeByte(type);
        Util.writeStreamable(data, outstream);
        Util.writeString(cluster_name, outstream);
        Util.writeViewId(view_id, outstream);
    }

    public void readFrom(DataInput instream) throws Exception {
        type=instream.readByte();
        data=(PingData)Util.readStreamable(PingData.class, instream);
        cluster_name=Util.readString(instream);
        view_id=Util.readViewId(instream);
    }
}
