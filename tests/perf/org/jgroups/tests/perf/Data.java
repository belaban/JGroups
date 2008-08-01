package org.jgroups.tests.perf;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Data sent around between members
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: Data.java,v 1.12 2008/08/01 09:11:35 belaban Exp $
 */
public class Data implements Streamable {
    final static byte DISCOVERY_REQ    = 1;
    final static byte DISCOVERY_RSP    = 2;
    final static byte DATA             = 3;
    final static byte RESULTS          = 4; // sent when a receiver has received all messages
    final static byte FINAL_RESULTS    = 5; // sent when a sender is done
    final static byte FINAL_RESULTS_OK = 6; // sent when we know the everyone has received FINAL_MSGS
    final static byte START            = 7; // start sending messages

    public Data() {
        ;
    }

    public Data(byte type) {
        this.type=type;
    }

    byte       type=0;
    byte[]     payload=null; // used with DATA
    boolean    sender=false; // used with DISCOVERY_RSP
    long       num_msgs=0;   // used with DISCOVERY_RSP
    MemberInfo result=null;  // used with RESULTS
    Map<Object,MemberInfo> results=null; // used with final results

    public int getType() {
        return type;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        if(payload != null) {
            out.writeBoolean(true);
            out.writeInt(payload.length);
            out.write(payload, 0, payload.length);
        }
        else
            out.writeBoolean(false);
        out.writeBoolean(sender);
        out.writeLong(num_msgs);

        Util.writeStreamable(result, out);

        if(results != null) {
            out.writeBoolean(true);
            out.writeInt(results.size());
            Object key;
            MemberInfo val;
            for(Map.Entry<Object,MemberInfo> entry: results.entrySet()) {
                key=entry.getKey();
                val=entry.getValue();
                try {
                    Util.writeObject(key, out);
                }
                catch(Exception e) {
                    throw new IOException("failed to write object " + key, e);
                }
                Util.writeStreamable(val, out);
            }
        }
        else
            out.writeBoolean(false);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        if(in.readBoolean()) {
            int length=in.readInt();
            payload=new byte[length];
            in.read(payload, 0, length);
        }
        sender=in.readBoolean();
        num_msgs=in.readLong();

        result=(MemberInfo)Util.readStreamable(MemberInfo.class, in);

        if(in.readBoolean()) {
            int length=in.readInt();
            results=new HashMap(length);
            Object key;
            MemberInfo val;
            for(int i=0; i < length; i++) {
                try {
                    key=Util.readObject(in);
                }
                catch(Exception e) {
                    IOException ex=new IOException("failed to read key");
                    ex.initCause(e);
                    throw ex;
                }
                val=(MemberInfo)Util.readStreamable(MemberInfo.class, in);
                results.put(key, val);
            }
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
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
        type=in.readByte();
        int len=in.readInt();
        if(len > 0) {
            payload=new byte[len];
            in.readFully(payload, 0, payload.length);
        }
        sender=in.readBoolean();
        num_msgs=in.readLong();
        boolean results_available=in.readBoolean();
        if(results_available)
            results=(Map)in.readObject();
    }




    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append('[');
        switch(type) {
        case DISCOVERY_REQ: sb.append("DISCOVERY_REQ"); break;
        case DISCOVERY_RSP: sb.append("DISCOVERY_RSP"); break;
        case DATA:          sb.append("DATA"); break;
        case RESULTS:       sb.append("RESULTS"); break;
        case FINAL_RESULTS: sb.append("FINAL_RESULTS"); break;
        case FINAL_RESULTS_OK: sb.append("FINAL_RESULTS_OK"); break;
        case START:            sb.append("START"); break;
        default:            sb.append("<unknown>"); break;
        }
        sb.append("] ");
        return sb.toString();
    }
}
