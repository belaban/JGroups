package org.jgroups.tests.perf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;

/**
 * Data sent around between members
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: Data.java,v 1.5 2004/07/05 06:18:36 belaban Exp $
 */
public class Data implements Externalizable {
    final static int DISCOVERY_REQ = 1;
    final static int DISCOVERY_RSP = 2;
    final static int DATA          = 3;
    final static int DONE          = 4; // sent when a sender is done
    final static int RESULTS       = 5; // sent when a receiver has received all messages

    public Data() {
        ;
    }

    public Data(int type) {
        this.type=type;
    }

    int     type=0;
    byte[]  payload=null; // used with DATA
    boolean sender=false; // used with DISCOVERY_RSP
    long    num_msgs=0;   // used with DISCOVERY_RSP
    HashMap results=null; // used with RESULTS

    public int getType() {
        return type;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type);
        if(payload != null) {
            out.writeInt(payload.length);
            out.write(payload, 0, payload.length);
        }
        else {
            out.writeInt(0);
        }
        out.writeBoolean(sender);
        out.writeLong(num_msgs);
        if(results != null) {
            out.writeBoolean(true);
            out.writeObject(results);
        }
        else
            out.writeBoolean(false);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type=in.readInt();
        int len=in.readInt();
        if(len > 0) {
            payload=new byte[len];
            in.readFully(payload, 0, payload.length);
        }
        sender=in.readBoolean();
        num_msgs=in.readLong();
        boolean results_available=in.readBoolean();
        if(results_available)
            results=(HashMap)in.readObject();
    }

//    public byte[] write() throws Exception {
//        byte[] retval=null;
//        switch(type) {
//            case DISCOVERY_REQ: return new byte[]{DISCOVERY_REQ};
//            case DISCOVERY_RSP:
//                byte[] tmp=Util.objectToByteBuffer(new Long(num_msgs));
//                retval=new byte[tmp.length +1];
//                retval[0]=DISCOVERY_RSP;
//                System.arraycopy(tmp, 0, retval, 1, tmp.length);
//                return retval;
//            case DATA:
//                retval=new byte[payload.length +1];
//                retval[0]=DATA;
//                System.arraycopy(payload, 0, retval, 1, payload.length);
//                return retval;
//            default: return new byte[]{(byte)type};
//        }
//    }
//
//    public void read(byte[] buf) throws Exception {
//        type=buf[0];
//        switch(type) {
//            case DISCOVERY_REQ:
//                break;
//            case DISCOVERY_RSP:
//                break;
//            case DATA:
//                payload=new byte[buf.length-1];
//                System.arraycopy(buf, 1, payload, 0, buf.length -1);
//                break;
//            default:
//                break;
//        }
//    }

    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append('[');
        switch(type) {
            case DISCOVERY_REQ: sb.append("DISCOVERY_REQ"); break;
            case DISCOVERY_RSP: sb.append("DISCOVERY_RSP"); break;
            case DATA:          sb.append("DATA"); break;
            case DONE:          sb.append("DONE"); break;
            case RESULTS:       sb.append("RESULTS"); break;
            default:            sb.append("<unknown>"); break;
        }
        sb.append("] ");
        return sb.toString();
    }
}
