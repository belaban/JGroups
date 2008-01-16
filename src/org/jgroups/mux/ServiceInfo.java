package org.jgroups.mux;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * Class used for service state communication between Multiplexers
 * @author Bela Ban
 * @version $Id: ServiceInfo.java,v 1.7.2.1 2008/01/16 09:15:14 vlada Exp $
 */
public class ServiceInfo implements Externalizable, Streamable {       
    public static final byte SERVICE_UP        = 3;
    public static final byte SERVICE_DOWN      = 4;
    public static final byte LIST_SERVICES_RSP = 5; // list of services available on a given node (available in 'state')
    public static final byte ACK               = 6;
    public static final byte SERVICES_MERGED   = 7;

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
            case SERVICE_UP:   return "SERVICE_UP(" + service + "," + host + ")";
            case SERVICE_DOWN: return "SERVICE_DOWN(" + service + "," + host + ")";
            case ACK: return "ACK";
            case SERVICES_MERGED: return "SERVICES_MERGED("+ host + ")";
            case LIST_SERVICES_RSP:
                String services=null;
                try {
                    services=Util.objectFromByteBuffer(state).toString();
                }
                catch(Exception e) {
                }
                return "LIST_SERVICES_RSP(" + services + ")";
            default: return "n/a";
        }
    }

    public static String typeToString(int t) {
        switch(t) {                       
            case SERVICE_UP:   return "SERVICE_UP";
            case SERVICE_DOWN: return "SERVICE_DOWN";
            case ACK:          return "ACK";
            case SERVICES_MERGED: return "SERVICES_MERGED";
            case LIST_SERVICES_RSP: return "LIST_SERVICES_RSP";
            default:           return "n/a";
        }
    }
}
