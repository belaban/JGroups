package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.UUID;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of SiteAddress
 * @author Bela Ban
 * @since 3.2
 */
public class SiteUUID extends UUID implements SiteAddress {
    private static final long serialVersionUID=-575018477146695139L;
    protected short site;
    // Maps between site-IDs (shorts) and site names
    protected static final ConcurrentMap<Short,String> site_cache=new ConcurrentHashMap<Short,String>(10);


    public SiteUUID() {
    }


    public SiteUUID(long mostSigBits, long leastSigBits, short site) {
        super(mostSigBits,leastSigBits);
        this.site=site;
    }

    public static void addToCache(short site, String name) {
        site_cache.putIfAbsent(site, name);
    }

    public static void replaceInCache(short site, String name) {
        site_cache.put(site, name);
    }

    public static void clearCache() {
        site_cache.clear();
    }
        
    public short getSite() {
        return site;
    }

    public int compareTo(Address other) {
        int retval=super.compareTo(other);
        if(retval != 0)
            return retval;
        SiteUUID tmp=(SiteUUID)other;
        return site == tmp.site ? 0: site < tmp.site? -1 : 1;
    }

    public UUID copy() {
        return new SiteUUID(mostSigBits, leastSigBits, site);
    }

    public boolean equals(Object obj) {
        return obj instanceof UUID && compareTo((Address)obj) == 0;
    }

    public int hashCode() {
        return super.hashCode() + site;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        site=in.readShort();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeShort(site);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        site=in.readShort();
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        out.writeShort(site);
    }

    public int size() {
        return super.size() + Global.SHORT_SIZE;
    }


    public String toString() {
        String retval=super.toString();
        String suffix=site_cache.get(site);
        return retval + ":" + (suffix != null? suffix : String.valueOf(site));
    }

    public String toStringLong() {
        String retval=super.toStringLong();
        String suffix=site_cache.get(site);
        return retval + ":" + (suffix != null? suffix : String.valueOf(site));
    }
}
