
package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;


/**
 * @author Bela Ban
 * @version $Id: PingHeader.java,v 1.14 2009/08/28 07:17:53 belaban Exp $
 */
public class PingHeader extends Header implements Streamable {
    public static final byte GET_MBRS_REQ=1;   // arg = null
    public static final byte GET_MBRS_RSP=2;   // arg = PingData (local_addr, coord_addr)

    public byte type=0;
    public PingData arg=null;
    public String cluster_name=null;
    // when set (with a GET_MBRS_REQ), we don't need the address mappings, but only the view
    public boolean return_view_only=false;
    private static final long serialVersionUID=3054979699998282428L;

    public PingHeader() {
    } // for externalization

    public PingHeader(byte type, String cluster_name) {
        this.type=type;
        this.cluster_name=cluster_name;
    }

    public PingHeader(byte type, PingData arg) {
        this.type=type;
        this.arg=arg;
    }

    public PingHeader(byte type, PingData arg, String cluster_name) {
        this(type, arg);
        this.cluster_name=cluster_name;
    }

    public int size() {
        int retval=Global.BYTE_SIZE *4; // type, presence and return_view_only
        if(arg != null) {
            retval+=arg.size();
        }
        if(cluster_name != null) {
            retval += cluster_name.length() +2;
        }
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[PING: type=" + type2Str(type));
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        if(arg != null)
            sb.append(", arg=" + arg);
        sb.append(']');
        return sb.toString();
    }

    static String type2Str(byte t) {
        switch(t) {
            case GET_MBRS_REQ:
                return "GET_MBRS_REQ";
            case GET_MBRS_RSP:
                return "GET_MBRS_RSP";
            default:
                return "<unkown type (" + t + ")>";
        }
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
        out.writeBoolean(return_view_only);
        out.writeObject(arg);
        out.writeObject(cluster_name);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        return_view_only=in.readBoolean();
        arg=(PingData)in.readObject();
        cluster_name=(String)in.readObject();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        outstream.writeByte(type);
        outstream.writeBoolean(return_view_only);
        Util.writeStreamable(arg, outstream);
        Util.writeString(cluster_name, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        type=instream.readByte();
        return_view_only=instream.readBoolean();
        arg=(PingData)Util.readStreamable(PingData.class, instream);
        cluster_name=Util.readString(instream);
    }
}
