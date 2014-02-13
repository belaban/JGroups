
package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.ViewId;
import org.jgroups.util.*;

import java.io.*;


/**
 * @author Bela Ban
 */
public class PingHeader extends Header {
    public static final byte GET_MBRS_REQ=1;   // arg = null
    public static final byte GET_MBRS_RSP=2;   // arg = PingData (local_addr, creator)

    public byte     type=0;
    public String   cluster_name;

    // when set, we don't need the address mappings, but only the view.
    // This is typically done for a merge-triggered discovery request
    public ViewId   view_id;

    
    public PingHeader() {
    }

    public PingHeader(byte type) {
        this.type=type;
    }

    public PingHeader clusterName(String name) {this.cluster_name=name; return this;}
    public PingHeader viewId(ViewId view_id)   {this.view_id=view_id; return this;}

    public int size() {
        int retval=Global.BYTE_SIZE *2; // type and cluster_name presence
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
        Bits.writeString(cluster_name,outstream);
        Util.writeViewId(view_id, outstream);
    }

    public void readFrom(DataInput instream) throws Exception {
        type=instream.readByte();
        cluster_name=Bits.readString(instream);
        view_id=Util.readViewId(instream);
    }
}
