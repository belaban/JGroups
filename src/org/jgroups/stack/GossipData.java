// $Id: GossipData.java,v 1.6 2009/07/03 15:15:29 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;


/**
 * Encapsulates data sent between GossipRouter and GossipClient
 * @author Bela Ban Oct 4 2001
 */
public class GossipData implements Streamable {
    byte           type=0;      // One of GossipRouter type, e.g. CONNECT, REGISTER etc
    String         group=null;  // CONNECT, GET_REQ and GET_RSP
    Address        addr=null;   // CONNECT
    List<Address>  mbrs=null;   // GET_RSP
    private Collection<PhysicalAddress> physical_addrs=null; // GET_RSP, GET_REQ

    public GossipData() { // for streamable
    }

    public GossipData(byte type) {
        this.type=type;
    }

    public GossipData(byte type, String group, Address addr, List<Address> mbrs) {
        this.type=type;
        this.group=group;
        this.addr=addr;
        this.mbrs=mbrs;
    }

    public GossipData(byte type, String group, Address addr, List<Address> mbrs, List<PhysicalAddress> physical_addrs) {
        this(type, group, addr, mbrs);
        this.physical_addrs=physical_addrs;
    }


    public byte             getType()    {return type;}
    public String           getGroup()   {return group;}
    public Address          getAddress() {return addr;}
    public List<Address>    getMembers() {return mbrs;}

    public void setMembers(List<Address> mbrs) {
        this.mbrs=mbrs;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(GossipRouter.type2String(type)).append( "(").append("group=").append(group).append(", addr=").append(addr);
        if(mbrs != null && !mbrs.isEmpty())
            sb.append(", mbrs=").append(mbrs);
        if(physical_addrs != null && !physical_addrs.isEmpty())
            sb.append(", physical_addrs=").append(Util.printListWithDelimiter(physical_addrs, ", "));
        return sb.toString();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        Util.writeString(group, out);
        Util.writeAddress(addr, out);
        Util.writeAddresses(mbrs, out);
        Util.writeAddresses(physical_addrs, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        group=Util.readString(in);
        addr=Util.readAddress(in);
        mbrs=(List<Address>)Util.readAddresses(in, LinkedList.class);
        physical_addrs=(Collection<PhysicalAddress>)Util.readAddresses(in, ArrayList.class);
    }


    public int size() {
        int retval=Global.BYTE_SIZE; // type
        retval+=Global.BYTE_SIZE;     // presence byte for group
        if(group != null)
            retval+=group.length() +2;
        retval+=Util.size(addr);
        retval+=Util.size(mbrs);
        retval+=Util.size(physical_addrs);
        return retval;
    }


}
