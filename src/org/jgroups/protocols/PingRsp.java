// $Id: PingRsp.java,v 1.15 2008/09/18 14:45:33 vlada Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class PingRsp implements Serializable, Streamable {

    private static final long serialVersionUID=3634334590904551586L;

    private Address own_addr=null;
    private Address coord_addr=null;
    private boolean is_server=false;

    public PingRsp() {
    // externalization
    }

    public PingRsp(Address own_addr,Address coord_addr,boolean is_server) {
        this.own_addr=own_addr;
        this.coord_addr=coord_addr;
        this.is_server=is_server;
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof PingRsp))
            return false;
        PingRsp other=(PingRsp)obj;
        return own_addr != null && other.own_addr != null && own_addr.equals(other.own_addr);
    }

    public int hashCode() {
        int retval=0;
        if(own_addr != null)
            retval+=own_addr.hashCode();
        if(coord_addr != null)
            retval+=coord_addr.hashCode();
        if(retval == 0)
            retval=super.hashCode();
        return retval;
    }

    public boolean isCoord() {
        return is_server && own_addr != null && coord_addr != null && own_addr.equals(coord_addr);
    }
    
    public boolean hasCoord(){
        return is_server && own_addr != null && coord_addr != null;
    }

    public int size() {
        int retval=Global.BYTE_SIZE * 3; // for is_server, plus 2 presence bytes
        if(own_addr != null) {
            retval+=Global.BYTE_SIZE; // 1 boolean for: IpAddress or other address ?
            retval+=own_addr.size();
        }
        if(coord_addr != null) {
            retval+=Global.BYTE_SIZE; // 1 boolean for: IpAddress or other address ?
            retval+=coord_addr.size();
        }
        return retval;
    }

    public Address getAddress() {
        return own_addr;
    }

    public Address getCoordAddress() {
        return coord_addr;
    }

    public boolean isServer() {
        return is_server;
    }

    public String toString() {
        return new StringBuilder("[own_addr=").append(own_addr)
                                              .append(", coord_addr=")
                                              .append(coord_addr)
                                              .append(", is_server=")
                                              .append(is_server)
                                              .append(", is_coord=" + isCoord())
                                              .append(']')
                                              .toString();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeAddress(own_addr, outstream);
        Util.writeAddress(coord_addr, outstream);
        outstream.writeBoolean(is_server);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        own_addr=Util.readAddress(instream);
        coord_addr=Util.readAddress(instream);
        is_server=instream.readBoolean();
    }
}
