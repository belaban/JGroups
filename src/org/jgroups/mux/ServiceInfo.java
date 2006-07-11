package org.jgroups.mux;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * Class used for service state communication between Multiplexers
 * @author Bela Ban
 * @version $Id: ServiceInfo.java,v 1.2 2006/07/11 05:48:26 belaban Exp $
 */
public class ServiceInfo implements Externalizable, Streamable {
    public static final byte STATE_REQ    = 1;
    public static final byte STATE_RSP    = 2;
    public static final byte SERVICE_UP   = 3;
    public static final byte SERVICE_DOWN = 4;

    byte    type=0;
    String  service=null;
    Address host=null;
    byte[]  state=null;


    public ServiceInfo() {
    }

    public ServiceInfo(byte type, String service, Address host, byte[] state) {
        this.type=type;
        this.service=service;
        this.host=host;
        this.state=state;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
          out.writeByte(type);
          out.writeUTF(service);
          out.writeObject(host);
          if(state != null) {
              out.writeInt(state.length);
              out.write(state);
          }
          else {
              out.writeInt(0);
          }
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
          type=in.readByte();
          service=in.readUTF();
          host=(Address)in.readObject();
          int len=in.readInt();
          if(len > 0) {
              state=new byte[len];
              in.readFully(state, 0, len);
          }
      }


      public long size() {
          long retval=Global.BYTE_SIZE; // type
          retval+=Global.BYTE_SIZE; // presence byte for service
          if(service != null)
              retval+=service.length() +2;
          retval+=Util.size(host);
          retval+=Global.INT_SIZE; // length of state
          if(state != null)
              retval+=state.length;
          return retval;
      }

      public void writeTo(DataOutputStream out) throws IOException {
          out.writeByte(type);
          Util.writeString(service, out);
          Util.writeAddress(host, out);
          if(state != null) {
              out.writeInt(state.length);
              out.write(state, 0, state.length);
          }
          else {
              out.writeInt(0);
          }
      }

      public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
          type=in.readByte();
          service=Util.readString(in);
          host=Util.readAddress(in);
          int len=in.readInt();
          if(len > 0) {
              state=new byte[len];
              in.readFully(state, 0, len);
          }
      }



    public String toString() {
        switch(type) {
            case STATE_REQ: return "STATE_REQ";
            case STATE_RSP:
                String tmp="STATE_RSP (";
                if(state == null)
                    tmp+=state;
                else
                    tmp+=state.length;
                tmp+=")";
                return tmp;
            case SERVICE_UP:   return "SERVICE_UP(" + service + "," + host + ")";
            case SERVICE_DOWN: return "SERVICE_DOWN(" + service + "," + host + ")";
            default: return "n/a";
        }
    }

    public static String typeToString(int t) {
        switch(t) {
            case STATE_REQ:    return "STATE_REQ";
            case STATE_RSP:    return "STATE_RSP";
            case SERVICE_UP:   return "SERVICE_UP";
            case SERVICE_DOWN: return "SERVICE_DOWN";
            default:           return "n/a";
        }
    }
}
