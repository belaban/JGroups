
package org.jgroups.stack;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Encapsulates data sent between GossipRouter and RouterStub (TCPGOSSIP and TUNNEL)
 * @author Bela Ban Oct 4 2001
 */
public class GossipData implements SizeStreamable {
    GossipType      type;
    String          group;         // REGISTER, GET_MBRS and GET_MBRS_RSP
    Address         addr;          // REGISTER
    Address         sender;        // MESSAGE (original sender of a message (not the GossipRouter!))
    PhysicalAddress physical_addr; // REGISTER, GET_MBRS, GET_MBRS_RSP
    String          logical_name;  // REGISTER
    List<PingData>  ping_data;     // GET_MBRS_RSP
    byte[]          buffer;        // MESSAGE
    int             offset;        // MESSAGE
    int             length;        // MESSAGE

    public GossipData() { // for streamable
    }

    public GossipData(GossipType type) {
        this.type=type;
    }

    public GossipData(GossipType type, String group, Address addr) {
        this(type);
        this.group=group;
        this.addr=addr;
    }

    public GossipData(GossipType type, String group, Address addr, List<PingData> ping_data) {
        this(type, group, addr);
        this.ping_data=ping_data;
    }

    public GossipData(GossipType type, String group, Address addr, List<PingData> ping_data, PhysicalAddress physical_addr) {
        this(type, group, addr, ping_data);
        this.physical_addr=physical_addr;
    }

    public GossipData(GossipType type, String group, Address addr, String logical_name, PhysicalAddress physical_addr) {
        this(type, group, addr);
        this.logical_name=logical_name;
        this.physical_addr=physical_addr;
    }

    public GossipData(GossipType type, String group, Address addr, byte[] buffer) {
        this(type, group, addr, buffer, 0, buffer.length);
    }

    public GossipData(GossipType type, String group, Address addr, byte[] buffer, int offset, int length) {
        this(type, group, addr);
        this.buffer=buffer;
        this.offset=offset;
        this.length=length;
    }


    public GossipType       getType()            {return type;}
    public String           getGroup()           {return group;}
    public Address          getAddress()         {return addr;}
    public Address          getSender()          {return sender;}
    public GossipData       setSender(Address s) {sender=s; return this;}
    public String           getLogicalName()     {return logical_name;}
    public List<PingData>   getPingData()        {return ping_data;}
    public byte[]           getBuffer()          {return buffer;}
    public int              getOffset()          {return offset;}
    public int              getLength()          {return length;}
    public PhysicalAddress  getPhysicalAddress() {return physical_addr;}
    public GossipData setPingData(List<PingData> mbrs) {
        this.ping_data=mbrs; return this;
    }

    public GossipData addPingData(PingData data) {
        if(ping_data == null)
            ping_data=new ArrayList<>();
        if(data != null)
            ping_data.add(data);
        return this;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(type).append( "(").append("group=").append(group).append(", addr=").append(addr);
        if(logical_name != null)
            sb.append(", logical_name=" + logical_name);
        if(ping_data != null && !ping_data.isEmpty())
            sb.append(", ping_data=").append(ping_data);
        if(physical_addr != null)
            sb.append(", physical_addr=").append(physical_addr);
        if(buffer != null)
            sb.append(", buffer: " + length + " bytes");
        sb.append(")");
        return sb.toString();
    }

    public int serializedSize() {
        int retval=Global.BYTE_SIZE;   // type
        if(group != null)
            retval+=group.length() +2; // group
        retval+=Global.BYTE_SIZE;      // presence for group
        retval+=Util.size(addr);       // addr
        retval+=Util.size(sender);

        if(type != GossipType.MESSAGE) {
            retval+=Global.BYTE_SIZE;     // presence byte for logical_name
            if(logical_name != null)
                retval+=logical_name.length() +2;

            retval+=Global.SHORT_SIZE;    // ping_data
            if(ping_data != null)
                for(PingData data: ping_data)
                    retval+=data.serializedSize();

            retval+=Util.size(physical_addr); // physical_addr
        }

        retval+=Global.INT_SIZE; // length of buffer
        if(buffer != null)
            retval+=length;
        return retval;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type.ordinal());
        Bits.writeString(group, out);
        Util.writeAddress(addr, out);
        Util.writeAddress(sender, out);

        if(type != GossipType.MESSAGE) {
            Bits.writeString(logical_name, out);
            out.writeShort(ping_data != null? ping_data.size() : 0);
            if(ping_data != null)
                for(PingData data : ping_data)
                    data.writeTo(out);

            Util.writeAddress(physical_addr, out);
        }

        out.writeInt(buffer != null? length : 0);
        if(buffer != null)
            out.write(buffer, offset, length);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        readFrom(in, true);
    }


    protected void readFrom(DataInput in, boolean read_type) throws IOException, ClassNotFoundException {
        if(read_type)
            type=GossipType.values()[in.readByte()];
        group=Bits.readString(in);
        addr=Util.readAddress(in);
        sender=Util.readAddress(in);

        if(type != GossipType.MESSAGE) {
            logical_name=Bits.readString(in);
            short len=in.readShort();
            if(len > 0) {
                ping_data=new ArrayList<>(len);
                for(int i=0; i < len; i++) {
                    PingData data=new PingData();
                    data.readFrom(in);
                    ping_data.add(data);
                }
            }
            physical_addr=(PhysicalAddress)Util.readAddress(in);
        }

        length=in.readInt();
        if(length > 0) {
            buffer=new byte[length];
            in.readFully(buffer, offset=0, length);
        }
    }



}
