// $Id: PingHeader.java,v 1.9 2005/04/15 13:17:02 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.Global;
import org.jgroups.util.Streamable;

import java.io.*;


public class PingHeader extends Header implements Streamable {
    public static final byte GET_MBRS_REQ=1;   // arg = null
    public static final byte GET_MBRS_RSP=2;   // arg = PingRsp(local_addr, coord_addr)

    public byte type=0;
    public PingRsp arg=null;

    public PingHeader() {
    } // for externalization

    public PingHeader(byte type, PingRsp arg) {
        this.type=type;
        this.arg=arg;
    }

    public long size() {
        long retval=Global.BYTE_SIZE *2; // type and presence
        if(arg != null) {
            retval+=arg.size();
        }
        return retval;
    }

    public String toString() {
        return "[PING: type=" + type2Str(type) + ", arg=" + arg + ']';
    }

    String type2Str(byte t) {
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
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        arg=(PingRsp)in.readObject();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        outstream.writeByte(type);
        if(arg != null) {
            outstream.write(1);
            arg.writeTo(outstream);
        }
        else {
            outstream.write(0);
        }
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        type=instream.readByte();
        int b=instream.read();
        if(b == 1) {
            arg=new PingRsp();
            arg.readFrom(instream);
        }
    }
}
