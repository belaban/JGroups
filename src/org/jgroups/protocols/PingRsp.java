// $Id: PingRsp.java,v 1.6 2005/01/05 10:39:28 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;


public class PingRsp implements Serializable, Streamable {
    public Address own_addr=null;
    public Address coord_addr=null;
    public boolean is_server=false;

    public PingRsp() {
        // externalization
    }

    public PingRsp(Address own_addr, Address coord_addr, boolean is_server) {
        this.own_addr=own_addr;
        this.coord_addr=coord_addr;
        this.is_server=is_server;
    }

    public boolean equals(Object obj) {
        PingRsp other=(PingRsp)obj;
        if(own_addr != null && other.own_addr != null && own_addr.equals(other.own_addr))
            return true;
        return false;
    }

    public boolean isCoord() {
        if(!is_server)
            return false;
        if(own_addr != null && coord_addr != null)
            return own_addr.equals(coord_addr);
        return false;
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
        return "[own_addr=" + own_addr + ", coord_addr=" + coord_addr + ", is_server=" + is_server + ']';
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
