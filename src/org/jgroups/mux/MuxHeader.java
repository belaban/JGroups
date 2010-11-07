package org.jgroups.mux;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.*;

/**
 * Header used for multiplexing and de-multiplexing between service components on top of a Multiplexer (Channel)
 * @author Bela Ban
 * @version $Id: MuxHeader.java,v 1.10 2010/06/15 06:44:36 belaban Exp $
 */
public class MuxHeader extends Header {
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

    public String getId() {
        return id;
    }



    public int size() {
        int retval=Global.BYTE_SIZE; // presence byte in Util.writeString
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
