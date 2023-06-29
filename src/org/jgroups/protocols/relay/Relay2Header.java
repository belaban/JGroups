package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Header for {@link RELAY2}
 * @author Bela Ban
 * @since  5.2.15
 */
public class Relay2Header extends Header {
    public static final byte DATA             = 1;
    public static final byte SITE_UNREACHABLE = 2; // final_dest is a SiteMaster
    public static final byte HOST_UNREACHABLE = 3; // final_dest is a SiteUUID (not currently used)
    public static final byte SITES_UP         = 4;
    public static final byte SITES_DOWN       = 5;

    protected byte        type;
    protected Address     final_dest;
    protected Address     original_sender;
    protected Set<String> sites; // used with SITES_UP/SITES_DOWN/TOPO_RSP
    protected Set<String> visited_sites; // used to record sites to which this msg was already sent
    // (https://issues.redhat.com/browse/JGRP-1519)


    public Relay2Header() {
    }

    public Relay2Header(byte type) {
        this.type=type;
    }

    public Relay2Header(byte type, Address final_dest, Address original_sender) {
        this(type);
        this.final_dest=final_dest;
        this.original_sender=original_sender;
    }
    public short        getMagicId()                   {return 80;}
    public Supplier<? extends Header> create()         {return Relay2Header::new;}
    public byte         getType()                      {return type;}
    public Address      getFinalDest()                 {return final_dest;}
    public Relay2Header setFinalDestination(Address d) {final_dest=d; return this;}
    public Address      getOriginalSender()            {return original_sender;}
    public Relay2Header setOriginalSender(Address s)   {original_sender=s; return this;}
    public Set<String>  getSites()                     {return sites;}
    public Relay2Header setSites(Set<String> s)        {sites=s; return this;}

    public Relay2Header setSites(String ... s) {
        if(s != null && s.length > 0) {
            if(this.sites == null)
                this.sites=new HashSet<>();
            this.sites.addAll(Arrays.asList(s));
        }
        return this;
    }

    public Relay2Header addToVisitedSites(String s) {
        if(visited_sites == null)
            visited_sites=new HashSet<>();
        visited_sites.add(s);
        return this;
    }

    public Relay2Header addToVisitedSites(Collection<String> list) {
        if(list == null || list.isEmpty())
            return this;
        for(String s: list)
            addToVisitedSites(s);
        return this;
    }

    public boolean     hasVisitedSites() {return visited_sites != null && !visited_sites.isEmpty();}
    public Set<String> getVisitedSites() {return visited_sites;}

    public Relay2Header copy() {
        Relay2Header hdr=new Relay2Header(type, final_dest, original_sender);
        if(this.sites != null)
            hdr.sites=new HashSet<>(this.sites);
        if(visited_sites != null) {
            hdr.addToVisitedSites(visited_sites);
        }
        return hdr;
    }

    @Override
    public int serializedSize() {
        return Global.BYTE_SIZE + Util.size(final_dest) + Util.size(original_sender) +
          sizeOf(sites) + sizeOf(visited_sites);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        Util.writeAddress(final_dest, out);
        Util.writeAddress(original_sender, out);
        out.writeInt(sites == null? 0 : sites.size());
        if(sites != null) {
            for(String s: sites)
                Bits.writeString(s, out);
        }
        out.writeInt(visited_sites == null? 0 : visited_sites.size());
        if(visited_sites != null) {
            for(String s: visited_sites)
                Bits.writeString(s, out);
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        final_dest=Util.readAddress(in);
        original_sender=Util.readAddress(in);
        int num_elements=in.readInt();
        if(num_elements > 0) {
            sites=new HashSet<>();
            for(int i=0; i < num_elements; i++)
                sites.add(Bits.readString(in));
        }
        num_elements=in.readInt();
        if(num_elements > 0) {
            visited_sites=new HashSet<>();
            for(int i=0; i < num_elements; i++)
                visited_sites.add(Bits.readString(in));
        }
    }

    public String toString() {
        return String.format("%s [final dest=%s, original sender=%s, sites=%s, visited=%s]",
                             typeToString(type), final_dest, original_sender, sites, visited_sites);
    }

    protected static String typeToString(byte type) {
        switch(type) {
            case DATA:             return "DATA";
            case SITE_UNREACHABLE: return "SITE_UNREACHABLE";
            case HOST_UNREACHABLE: return "HOST_UNREACHABLE";
            case SITES_UP:         return "SITES_UP";
            case SITES_DOWN:       return "SITES_DOWN";
            default:               return "<unknown>";
        }
    }

    protected static int sizeOf(Collection<String> list) {
        int retval=Global.INT_SIZE; // number of elements
        if(list != null) {
            for(String s: list)
                retval+=Bits.sizeUTF(s) + 1; // presence bytes
        }
        return retval;
    }
}