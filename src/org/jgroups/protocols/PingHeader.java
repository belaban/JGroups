// $Id: PingHeader.java,v 1.13 2009/04/09 09:11:15 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;


public class PingHeader extends Header implements Streamable {
    public static final byte GET_MBRS_REQ=1;   // arg = null
    public static final byte GET_MBRS_RSP=2;   // arg = PingData (local_addr, coord_addr)

    public byte type=0;
    public PingData arg=null;
    public String cluster_name=null;
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
        int retval=Global.BYTE_SIZE *3; // type and presence
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
        out.writeObject(arg);
        out.writeObject(cluster_name);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        arg=(PingData)in.readObject();
        cluster_name=(String)in.readObject();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        outstream.writeByte(type);
        Util.writeStreamable(arg, outstream);
        Util.writeString(cluster_name, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        type=instream.readByte();
        arg=(PingData)Util.readStreamable(PingData.class, instream);
        cluster_name=Util.readString(instream);
    }
}
