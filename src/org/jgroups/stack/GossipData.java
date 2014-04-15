
package org.jgroups.stack;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;


/**
 * Encapsulates data sent between GossipRouter and GossipClient
 * @author Bela Ban Oct 4 2001
 */
public class GossipData implements SizeStreamable {
    byte            type;          // One of GossipRouter type, e.g. CONNECT, REGISTER etc
    String          group;         // CONNECT, GET_REQ and GET_RSP
    Address         addr;          // CONNECT
    String          logical_name;
    List<Address>   mbrs;          // GET_RSP
    PhysicalAddress physical_addr; // GET_RSP, GET_REQ
    byte[]          buffer;        // MESSAGE
    int             offset;
    int             length;

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

    /** @deprecated Use the constructor wityh a single PhysicalAddress instead */
    @Deprecated
    public GossipData(byte type, String group, Address addr, List<Address> mbrs, List<PhysicalAddress> physical_addrs) {
        this(type, group, addr, mbrs);
        if(physical_addrs != null && !physical_addrs.isEmpty())
            physical_addr=physical_addrs.get(0);
    }

    public GossipData(byte type, String group, Address addr, List<Address> mbrs, PhysicalAddress physical_addr) {
        this(type, group, addr, mbrs);
        this.physical_addr=physical_addr;
    }

    /** @deprecated Use the constructor wityh a single PhysicalAddress instead */
    @Deprecated
    public GossipData(byte type, String group, Address addr, String logical_name, List<PhysicalAddress> physical_addrs) {
        this(type, group, addr);
        this.logical_name=logical_name;
        if(physical_addrs != null && !physical_addrs.isEmpty())
            physical_addr=physical_addrs.get(0);
    }

    public GossipData(byte type, String group, Address addr, String logical_name, PhysicalAddress physical_addr) {
        this(type, group, addr);
        this.logical_name=logical_name;
        this.physical_addr=physical_addr;
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


    public byte             getType()        {return type;}
    public String           getGroup()       {return group;}
    public Address          getAddress()     {return addr;}
    public String           getLogicalName() {return logical_name;}
    public List<Address>    getMembers()     {return mbrs;}
    public byte[]           getBuffer()      {return buffer;}

    /** @deprecated Use {@link #getPhysicalAddress()} instead */
    @Deprecated
    public Collection<PhysicalAddress> getPhysicalAddresses() {return Arrays.asList(physical_addr);}

    public PhysicalAddress getPhysicalAddress() {return physical_addr;}

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
        if(physical_addr != null)
            sb.append(", physical_addr=").append(physical_addr);
        if(buffer != null)
            sb.append(", buffer: " + length + " bytes");
        sb.append(")");
        return sb.toString();
    }


    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        Bits.writeString(group,out);
        Util.writeAddress(addr, out);
        Bits.writeString(logical_name,out);
        Util.writeAddresses(mbrs, out);
        Util.writeAddress(physical_addr, out);
        Util.writeByteBuffer(buffer, offset, length, out);
    }

    public void readFrom(DataInput in) throws Exception {
        type=in.readByte();
        group=Bits.readString(in);
        addr=Util.readAddress(in);
        logical_name=Bits.readString(in);
        mbrs=(List<Address>)Util.readAddresses(in, LinkedList.class);
        physical_addr=(PhysicalAddress)Util.readAddress(in);
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
        retval+=Util.size(physical_addr);
        if(buffer != null)
        retval+=Global.INT_SIZE + length;
        return retval;
    }


}
