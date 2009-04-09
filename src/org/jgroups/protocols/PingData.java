
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.util.Streamable;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * Encapsulates information about a cluster node, e.g. local address, coordinator's addresss, logical name and
 * physical address(es)
 * @author Bela Ban
 * @version $Id: PingData.java,v 1.2 2009/04/09 09:11:15 belaban Exp $
 */
public class PingData implements Streamable {
    private Address own_addr=null;
    private Address coord_addr=null;
    private boolean is_server=false;
    private String logical_name=null;
    private List<PhysicalAddress> physical_addrs=null;


    public PingData() {
    }

    public PingData(Address own_addr, Address coord_addr, boolean is_server) {
        this.own_addr=own_addr;
        this.coord_addr=coord_addr;
        this.is_server=is_server;
    }


    public PingData(Address own_addr, Address coord_addr, boolean is_server,
                    String logical_name, List<PhysicalAddress> physical_addrs) {
        this(own_addr, coord_addr, is_server);
        this.logical_name=logical_name;
        if(physical_addrs != null) {
            this.physical_addrs=new ArrayList<PhysicalAddress>(physical_addrs);
        }
    }


    public boolean isCoord() {
        return is_server && own_addr != null && coord_addr != null && own_addr.equals(coord_addr);
    }
    
    public boolean hasCoord(){
        return is_server && own_addr != null && coord_addr != null;
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

    public String getLogicalName() {
        return logical_name;
    }

    public List<PhysicalAddress> getPhysicalAddrs() {
        return physical_addrs;
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof PingData))
            return false;
        PingData other=(PingData)obj;
        return own_addr != null && own_addr.equals(other.own_addr);
    }

    public int hashCode() {
        int retval=0;
        if(own_addr != null)
            retval+=own_addr.hashCode();
        if(retval == 0)
            retval=super.hashCode();
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("own_addr=").append(own_addr).append(", coord_addr=").append(coord_addr)
                .append(", is_server=").append(is_server).append(", is_coord=" + isCoord());
        if(logical_name != null)
            sb.append(", logical_name=").append(logical_name);
        if(physical_addrs != null && !physical_addrs.isEmpty())
            sb.append(", physical_addrs=").append(Util.printListWithDelimiter(physical_addrs, ", "));
        return sb.toString();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeAddress(own_addr, outstream);
        Util.writeAddress(coord_addr, outstream);
        outstream.writeBoolean(is_server);
        Util.writeString(logical_name, outstream);
        Util.writeAddresses(physical_addrs, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        own_addr=Util.readAddress(instream);
        coord_addr=Util.readAddress(instream);
        is_server=instream.readBoolean();
        logical_name=Util.readString(instream);
        physical_addrs=(List<PhysicalAddress>)Util.readAddresses(instream, ArrayList.class);
    }

    public int size() {
        int retval=Global.BYTE_SIZE; // for is_server

        retval+=Util.size(own_addr);
        retval+=Util.size(coord_addr);
        retval+=Global.BYTE_SIZE;     // presence byte for logical_name
        if(logical_name != null)
            retval+=logical_name.length() +2;

        retval+=Util.size(physical_addrs);

        return retval;
    }
}
