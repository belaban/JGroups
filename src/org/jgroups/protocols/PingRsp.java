// $Id: PingRsp.java,v 1.5 2004/10/04 20:43:31 belaban Exp $

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

    public PingRsp() {
        // externalization
    }

    public PingRsp(Address own_addr, Address coord_addr) {
        this.own_addr=own_addr;
        this.coord_addr=coord_addr;
    }

    public boolean isCoord() {
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

    public String toString() {
        return "[own_addr=" + own_addr + ", coord_addr=" + coord_addr + ']';
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeAddress(own_addr, outstream);
        Util.writeAddress(coord_addr, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        own_addr=Util.readAddress(instream);
        coord_addr=Util.readAddress(instream);
    }
}
