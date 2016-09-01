package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.*;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Implementation of SiteAddress
 * @author Bela Ban
 * @since 3.2
 */
public class SiteUUID extends ExtendedUUID implements SiteAddress {
    protected static final byte[] NAME      = Util.stringToBytes("relay2.name"); // logical name, can be null
    protected static final byte[] SITE_NAME = Util.stringToBytes("relay2.site");


    public SiteUUID() {
    }

    public SiteUUID(long mostSigBits, long leastSigBits, String name, String site) {
        super(mostSigBits,leastSigBits);
        if(name != null)
            put(NAME, Util.stringToBytes(name));
        put(SITE_NAME, Util.stringToBytes(site));
    }

    public SiteUUID(long mostSigBits, long leastSigBits, byte[] name, byte[] site) {
        super(mostSigBits,leastSigBits);
        if(name != null)
            put(NAME, name);
        put(SITE_NAME, site);
    }

    public SiteUUID(UUID uuid, String name, String site) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        if(name != null)
            put(NAME, Util.stringToBytes(name));
        put(SITE_NAME, Util.stringToBytes(site));
    }

    public Supplier<? extends UUID> create() {
        return SiteUUID::new;
    }

    public String getName() {
        return Util.bytesToString(get(NAME));
    }

    public String getSite() {
        return Util.bytesToString(get(SITE_NAME));
    }

    public UUID copy() {
        return new SiteUUID(mostSigBits, leastSigBits, get(NAME), get(SITE_NAME));
    }

    @Override
    public String toString() {
        return print(false);
    }

    @Override
    public int compareTo(Address other) {
        if (other instanceof SiteUUID) {
            int siteCompare = Util.compare(get(SITE_NAME), ((SiteUUID) other).get(SITE_NAME));
            //compareTo will check the bits.
            return siteCompare == 0 ? super.compareTo(other) : siteCompare;
        }
        return super.compareTo(other);
    }

    public String print(boolean detailed) {
        String name=getName();
        String retval=name != null? name : NameCache.get(this);
        return retval + ":" + getSite() + (detailed? printOthers() : "");
    }

    protected String printOthers() {
        StringBuilder sb=new StringBuilder();
        if(flags != 0)
            sb.append(" flags=" + flags + " (" + flags + ")");
        if(keys == null)
            return sb.toString();
        for(int i=0; i < keys.length; i++) {
            byte[] key=keys[i];
            if(key == null || Arrays.equals(key,SITE_NAME) || Arrays.equals(key, NAME))
                continue;
            byte[] val=values[i];
            Object obj=null;
            try {
                obj=Util.bytesToString(val);
            }
            catch(Throwable t) {
                obj=val != null? val.length + " bytes" : null;
            }
            sb.append(", ").append(new AsciiString(key)).append("=").append(obj);
        }
        return sb.toString();
    }
}
