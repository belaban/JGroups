// $Id: PingHeader.java,v 1.7 2004/10/08 13:15:46 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.util.Streamable;

import java.io.*;


public class PingHeader extends Header implements Streamable {
    public static final short GET_MBRS_REQ=1;   // arg = null
    public static final short GET_MBRS_RSP=2;   // arg = PingRsp(local_addr, coord_addr)

    public short type=0;
    public PingRsp arg=null;

    public PingHeader() {
    } // for externalization

    public PingHeader(short type, PingRsp arg) {
        this.type=type;
        this.arg=arg;
    }

    public long size() {
        return 100;
    }

    public String toString() {
        return "[PING: type=" + type2Str(type) + ", arg=" + arg + ']';
    }

    String type2Str(int t) {
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
        out.writeShort(type);
        out.writeObject(arg);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readShort();
        arg=(PingRsp)in.readObject();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        outstream.writeShort(type);
        if(arg != null) {
            outstream.write(1);
            arg.writeTo(outstream);
        }
        else {
            outstream.write(0);
        }
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        type=instream.readShort();
        int b=instream.read();
        if(b == 1) {
            arg=new PingRsp();
            arg.readFrom(instream);
        }
    }
}
