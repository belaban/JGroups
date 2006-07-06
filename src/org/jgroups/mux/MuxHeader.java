package org.jgroups.mux;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * Header used for multiplexing and de-multiplexing between service components on top of a Multiplexer (Channel)
 * @author Bela Ban
 * @version $Id: MuxHeader.java,v 1.5 2006/07/06 12:26:54 belaban Exp $
 */
public class MuxHeader extends Header implements Streamable {
    String      id=null;

    /** Used for service state communication between Multiplexers */
    ServiceInfo info;

    public MuxHeader() {
    }

    public MuxHeader(String id) {
        this.id=id;
    }

    public MuxHeader(ServiceInfo info) {
        this.info=info;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(id);
        out.writeObject(info);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id=in.readUTF();
        info=(ServiceInfo)in.readObject();
    }


    public long size() {
        long retval=Global.BYTE_SIZE; // presence byte in Util.writeString
        if(id != null)
            retval+=id.length() +2;   // for UTF
        retval+=Global.BYTE_SIZE;     // presence for info
        if(info != null)
            retval+=info.size();
        return retval;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeString(id, out);
        if(info != null) {
            out.writeBoolean(true);
            info.writeTo(out);
        }
        else {
            out.writeBoolean(false);
        }
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        id=Util.readString(in);
        if(in.readBoolean()) {
            info=new ServiceInfo();
            info.readFrom(in);
        }
    }

    public String toString() {
        if(id != null)
            return id;
        if(info != null)
            return info.toString();
        return "";
    }
}
