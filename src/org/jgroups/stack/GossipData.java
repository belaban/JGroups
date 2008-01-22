// $Id: GossipData.java,v 1.3.6.1 2008/01/22 10:01:10 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;


/**
 * Encapsulates data sent between GossipRouter and GossipClient
 * @author Bela Ban Oct 4 2001
 */
public class GossipData implements Streamable {
    byte    type=0;      // One of GossipRouter type, e.g. CONNECT, REGISTER etc
    String  group=null;  // CONNECT, GET_REQ and GET_RSP
    Address addr=null;   // CONNECT
    List    mbrs=null;   // GET_RSP

    public GossipData() { // for streamable
    }

    public GossipData(byte type) {
        this.type=type;
    }

    public GossipData(byte type, String group, Address addr, List mbrs) {
        this.type=type;
        this.group=group;
        this.addr=addr;
        this.mbrs=mbrs;
    }


    public byte    getType()    {return type;}
    public String  getGroup()   {return group;}
    public Address getAddress() {return addr;}
    public List    getMembers() {return mbrs;}

    public void setMembers(List mbrs) {
        this.mbrs=mbrs;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(GossipRouter.type2String(type)).append( "(").append("group=").append(group).append(", addr=").append(addr);
        sb.append(", mbrs=").append(mbrs);
        return sb.toString();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        Util.writeString(group, out);
        Util.writeAddress(addr, out);
        Util.writeAddresses(mbrs, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        group=Util.readString(in);
        addr=Util.readAddress(in);
        mbrs=(List)Util.readAddresses(in, LinkedList.class);
    }


}
