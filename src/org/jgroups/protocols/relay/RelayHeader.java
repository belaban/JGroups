package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

/**
 * Header for {@link RELAY2} and {@link RELAY3}
 * @author Bela Ban
 * @since  5.2.15
 */
public class RelayHeader extends Header {
    public static final byte DATA             = 1;
    public static final byte SITE_UNREACHABLE = 2; // final_dest is a SiteMaster
    public static final byte SITES_UP         = 4;
    public static final byte SITES_DOWN       = 5;
    public static final byte TOPO_REQ         = 6;
    public static final byte TOPO_RSP         = 7; // View is the payload of the response message (site: hdr.sites)

    protected byte        type;
    protected Address     final_dest;
    protected Address     original_sender;
    protected Set<String> sites; // used with SITES_UP/SITES_DOWN/TOPO_RSP
    // used to record sites to which this msg was already sent (https://issues.redhat.com/browse/JGRP-1519)
    protected Set<String> visited_sites;
    // used with TOPO_REQ: when set, return the entire cache, otherwise only information about the local members
    protected boolean     return_entire_cache;


    public RelayHeader() {
    }

    public RelayHeader(byte type) {
        this.type=type;
    }

    public RelayHeader(byte type, Address final_dest, Address original_sender) {
        this(type);
        this.final_dest=final_dest;
        this.original_sender=original_sender;
    }

    public short        getMagicId()                   {return 80;}
    public Supplier<? extends Header> create()         {return RelayHeader::new;}
    public byte         getType()                      {return type;}
    public Address      getFinalDest()                 {return final_dest;}
    public RelayHeader  setFinalDestination(Address d) {final_dest=d; return this;}
    public Address      getOriginalSender()            {return original_sender;}
    public RelayHeader  setOriginalSender(Address s)   {original_sender=s; return this;}
    public Set<String>  getSites()                     {return sites != null? new HashSet<>(sites) : null;}
    public boolean      hasSites()                     {return sites != null && !sites.isEmpty();}
    public boolean      returnEntireCache()            {return return_entire_cache;}
    public RelayHeader  returnEntireCache(boolean b)   {return_entire_cache=b; return this;}

    public String getSite() {
        if(sites == null || sites.isEmpty())
            return null;
        Iterator<String> it=sites.iterator();
        return it.hasNext()? it.next() : null;
    }

    public RelayHeader addToSites(Collection<String> s) {
        if(s != null) {
            if(this.sites == null)
                this.sites=new HashSet<>(s.size());
            this.sites.addAll(s);
        }
        assertNonNullSites();
        return this;
    }

    public RelayHeader addToSites(String ... s) {
        if(s != null && s.length > 0) {
            if(this.sites == null)
                this.sites=new HashSet<>();
            this.sites.addAll(Arrays.asList(s));
        }
        assertNonNullSites();
        return this;
    }

    public RelayHeader addToVisitedSites(String s) {
        if(visited_sites == null)
            visited_sites=new HashSet<>();
        visited_sites.add(s);
        return this;
    }

    public RelayHeader addToVisitedSites(Collection<String> list) {
        if(list == null || list.isEmpty())
            return this;
        for(String s: list)
            addToVisitedSites(s);
        return this;
    }

    public boolean     hasVisitedSites() {return visited_sites != null && !visited_sites.isEmpty();}
    public Set<String> getVisitedSites() {return visited_sites;}

    public RelayHeader copy() {
        RelayHeader hdr=new RelayHeader(type, final_dest, original_sender)
          .addToSites(this.sites)
          .addToVisitedSites(visited_sites).returnEntireCache(return_entire_cache);
        assertNonNullSites();
        hdr.assertNonNullSites();
        return hdr;
    }

    @Override
    public int serializedSize() {
        assertNonNullSites();
        return Global.BYTE_SIZE*2 + Util.size(final_dest) + Util.size(original_sender) +
          sizeOf(sites) + sizeOf(visited_sites);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        out.writeBoolean(return_entire_cache);
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
        assertNonNullSites();
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        type=in.readByte();
        return_entire_cache=in.readBoolean();
        final_dest=Util.readAddress(in);
        original_sender=Util.readAddress(in);
        int num_elements=in.readInt();
        if(num_elements > 0) {
            sites=new HashSet<>(num_elements);
            for(int i=0; i < num_elements; i++)
                sites.add(Bits.readString(in));
        }
        num_elements=in.readInt();
        if(num_elements > 0) {
            visited_sites=new HashSet<>(num_elements);
            for(int i=0; i < num_elements; i++)
                visited_sites.add(Bits.readString(in));
        }
        assertNonNullSites();
    }

    public String toString() {
        return String.format("%s [final dest=%s, original sender=%s%s%s]",
                             typeToString(type), final_dest, original_sender,
                             sites == null || sites.isEmpty()? "" : String.format(", sites=%s", sites),
                             visited_sites == null || visited_sites.isEmpty()? "" :
                               String.format(", visited=%s", visited_sites));
    }

    protected static String typeToString(byte type) {
        switch(type) {
            case DATA:             return "DATA";
            case SITE_UNREACHABLE: return "SITE_UNREACHABLE";
            case SITES_UP:         return "SITES_UP";
            case SITES_DOWN:       return "SITES_DOWN";
            case TOPO_REQ:         return "TOPO_REQ";
            case TOPO_RSP:         return "TOPO_RSP";
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

    protected void assertNonNullSites() {
        if(type == SITES_UP || type == SITES_DOWN) {
            if(sites == null)
                throw new IllegalStateException(String.format("sites cannot be null with type %s\n", type));
        }
    }

}