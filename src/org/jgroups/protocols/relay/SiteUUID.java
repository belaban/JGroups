package org.jgroups.protocols.relay;

import org.jgroups.Global;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of SiteAddress
 * @author Bela Ban
 * @since 3.2
 */
public class SiteUUID extends UUID implements SiteAddress {
    private static final long serialVersionUID=3748908939644729773L;
    protected String name; // logical name, can be null

    protected short site;

    // Maps between site-IDs (shorts) and site names
    protected static final ConcurrentMap<Short,String> site_cache=new ConcurrentHashMap<Short,String>(10);


    public SiteUUID() {
    }


    public SiteUUID(long mostSigBits, long leastSigBits, String name, short site) {
        super(mostSigBits,leastSigBits);
        this.name=name;
        this.site=site;
    }

    public SiteUUID(UUID uuid, String name, short site) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.name=name;
        this.site=site;
    }

    public String getName() {
        return name;
    }

    public static void addToCache(short site, String name) {
        site_cache.putIfAbsent(site, name);
    }

    public static void replaceInCache(short site, String name) {
        site_cache.put(site, name);
    }

    public static Collection<String> cacheValues() {return site_cache.values();}

    public static boolean hasCacheValues() {return !site_cache.isEmpty();}

    public static String getSiteName(short site) {
        return site_cache.get(site);
    }

    public static void clearCache() {
        site_cache.clear();
    }

    public short getSite() {
        return site;
    }

//    public int compareTo(Address other) {
//        int retval=super.compareTo(other);
//        if(retval != 0)
//            return retval;
//        SiteUUID tmp=(SiteUUID)other;
//        return site == tmp.site ? 0: site < tmp.site? -1 : 1;
//    }

    public UUID copy() {
        return new SiteUUID(mostSigBits, leastSigBits, name, site);
    }

//    public boolean equals(Object obj) {
//        return obj instanceof UUID && compareTo((Address)obj) == 0;
//    }

//    public int hashCode() {
//        return super.hashCode() + site;
//    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        name=in.readUTF();
        site=in.readShort();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(name);
        out.writeShort(site);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        name=Util.readString(in);
        site=in.readShort();
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Util.writeString(name, out);
        out.writeShort(site);
    }

    public int size() {
        return super.size() + Util.size(name) + Global.SHORT_SIZE;
    }


    public String toString() {
        String retval=name != null? name : super.toString();
        String suffix=site_cache.get(site);
        return retval + ":" + (suffix != null? suffix : String.valueOf(site));
    }

    protected static short getSite(String site_name) {
        for(Map.Entry<Short,String> entry: site_cache.entrySet()) {
            if(entry.getValue().equals(site_name))
                return entry.getKey();
        }
        throw new IllegalArgumentException("site \"" + site_name + "\" does not have a corresponding ID");
    }
}
