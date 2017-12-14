
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Constructable;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Encapsulates information about a cluster node, e.g. local address, coordinator's address, logical name and
 * physical address(es)
 * @author Bela Ban
 */
public class PingData implements SizeStreamable, Constructable<PingData> {
    protected Address                       sender;  // the sender of this PingData
    protected byte                          flags;   // used to mark as server and/or coordinator
    protected String                        logical_name;
    protected PhysicalAddress               physical_addr;
    protected Collection<? extends Address> mbrs; // list of members to find, sent with discovery request (can be null)

    protected static final byte is_server = 1;
    protected static final byte is_coord  = 1 << 1;


    public PingData() {
    }

    public PingData(Address sender, boolean is_server) {
        this.sender=sender;
        server(is_server);
    }


    public PingData(Address sender, boolean is_server, String logical_name, PhysicalAddress physical_addr) {
        this(sender, is_server);
        this.logical_name=logical_name;
        this.physical_addr=physical_addr;
    }

    public Supplier<? extends PingData> create() {
        return PingData::new;
    }

    public PingData coord(boolean c) {
        if(c) {
            flags=Util.setFlag(flags, is_coord);
            flags=Util.setFlag(flags, is_server); // coord has to be a server
        }
        else
            flags=Util.clearFlags(flags, is_coord);
        return this;
    }

    public PingData server(boolean c) {
        if(c)
            flags=Util.setFlag(flags, is_server);
        else
            flags=Util.clearFlags(flags, is_server);
        return this;
    }

    public boolean isCoord() {
        return Util.isFlagSet(flags, is_coord);
    }
    
    public boolean isServer() {
        return Util.isFlagSet(flags, is_server) || Util.isFlagSet(flags, is_coord); // a coord is always a server
    }

    public Address getAddress() {
        return sender;
    }

    public String getLogicalName() {
        return logical_name;
    }

    public PhysicalAddress               getPhysicalAddr()                        {return physical_addr;}
    public PingData                      mbrs(Collection<? extends Address> mbrs) {this.mbrs=mbrs; return this;}
    public Collection<? extends Address> mbrs()                                   {return mbrs;}


    public boolean equals(Object obj) {
        if(!(obj instanceof PingData))
            return false;
        PingData other=(PingData)obj;
        return Objects.equals(sender, other.sender);
    }

    public int hashCode() {
        int retval=0;
        if(sender != null)
            retval+=sender.hashCode();
        if(retval == 0)
            retval=super.hashCode();
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(sender);
        if(logical_name != null)
            sb.append(", name=").append(logical_name);
        if(physical_addr != null)
            sb.append(", addr=").append(physical_addr);
        if(isCoord())
            sb.append(", coord");
        else if(isServer())
            sb.append(", server");
        if(mbrs != null)
            sb.append(", mbrs=" + mbrs.size());
        return sb.toString();
    }

    public int serializedSize() {
        int retval=Global.BYTE_SIZE; // for is_server
        retval+=Util.size(sender);
        retval+=Global.BYTE_SIZE;     // presence byte for logical_name
        if(logical_name != null)
            retval+=logical_name.length() +2;
        retval+=Util.size(physical_addr);
        retval+=Util.size(mbrs);
        return retval;
    }

    public void writeTo(DataOutput outstream) throws Exception {
        Util.writeAddress(sender, outstream);
        outstream.writeByte(flags);
        Bits.writeString(logical_name,outstream);
        Util.writeAddress(physical_addr,outstream);
        Util.writeAddresses(mbrs, outstream);
    }

    public void readFrom(DataInput instream) throws Exception {
        sender=Util.readAddress(instream);
        flags=instream.readByte();
        logical_name=Bits.readString(instream);
        physical_addr=(PhysicalAddress)Util.readAddress(instream);
        mbrs=Util.readAddresses(instream, ArrayList::new);
    }


}
