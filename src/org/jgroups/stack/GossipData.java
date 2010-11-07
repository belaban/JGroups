// $Id: GossipData.java,v 1.7 2009/07/08 15:30:31 belaban Exp $

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
    String         logical_name=null;
    List<Address>  mbrs=null;   // GET_RSP
    Collection<PhysicalAddress> physical_addrs=null; // GET_RSP, GET_REQ
    byte[]         buffer=null; // MESSAGE
    int            offset=0;
    int            length=0;

    public GossipData() { // for streamable
    }

    public GossipData(byte type) {
        this.type=type;
    }

    public GossipData(byte type, String group, Address addr) {
        this(type);
        this.group=group;
        this.addr=addr;
    }

    public GossipData(byte type, String group, Address addr, List<Address> mbrs) {
        this(type, group, addr);
        this.mbrs=mbrs;
    }

    public GossipData(byte type, String group, Address addr, List<Address> mbrs, List<PhysicalAddress> physical_addrs) {
        this(type, group, addr, mbrs);
        this.physical_addrs=physical_addrs;
    }

    public GossipData(byte type, String group, Address addr, String logical_name, List<PhysicalAddress> phys_addrs) {
        this(type, group, addr);
        this.logical_name=logical_name;
        this.physical_addrs=phys_addrs;
    }

    public GossipData(byte type, String group, Address addr, byte[] buffer) {
        this(type, group, addr, buffer, 0, buffer.length);
    }

    public GossipData(byte type, String group, Address addr, byte[] buffer, int offset, int length) {
        this(type, group, addr);
        this.buffer=buffer;
        this.offset=offset;
        this.length=length;
    }


    public byte             getType()    {return type;}
    public String           getGroup()   {return group;}
    public Address          getAddress() {return addr;}
    public String           getLogicalName() {return logical_name;}
    public List<Address>    getMembers() {return mbrs;}
    public byte[]           getBuffer()  {return buffer;}

    public Collection<PhysicalAddress> getPhysicalAddresses() {
        return physical_addrs;
    }

    public void setMembers(List<Address> mbrs) {
        this.mbrs=mbrs;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(GossipRouter.type2String(type)).append( "(").append("group=").append(group).append(", addr=").append(addr);
        if(logical_name != null)
        sb.append(", logical_name=" + logical_name);
        if(mbrs != null && !mbrs.isEmpty())
            sb.append(", mbrs=").append(mbrs);
        if(physical_addrs != null && !physical_addrs.isEmpty())
            sb.append(", physical_addrs=").append(Util.printListWithDelimiter(physical_addrs, ", "));
        if(buffer != null)
            sb.append(", buffer: " + length + " bytes");
        sb.append(")");
        return sb.toString();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        Util.writeString(group, out);
        Util.writeAddress(addr, out);
        Util.writeString(logical_name, out);
        Util.writeAddresses(mbrs, out);
        Util.writeAddresses(physical_addrs, out);
        Util.writeByteBuffer(buffer, offset, length, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        group=Util.readString(in);
        addr=Util.readAddress(in);
        logical_name=Util.readString(in);
        mbrs=(List<Address>)Util.readAddresses(in, LinkedList.class);
        physical_addrs=(Collection<PhysicalAddress>)Util.readAddresses(in, ArrayList.class);
        buffer=Util.readByteBuffer(in);
        if(buffer != null) {
            offset=0;
            length=buffer.length;
        }
    }


    public int size() {
        int retval=Global.BYTE_SIZE; // type
        retval+=Global.BYTE_SIZE * 3;     // presence byte for group and logical_name, buffer
        if(group != null)
            retval+=group.length() +2;
        retval+=Util.size(addr);
        if(logical_name != null)
            retval+=logical_name.length() +2;
        retval+=Util.size(mbrs);
        retval+=Util.size(physical_addrs);
        if(buffer != null)
        retval+=Global.INT_SIZE + length;
        return retval;
    }


}
