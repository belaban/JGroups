package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Implementation of SiteAddress
 * @author Bela Ban
 * @since 3.2
 */
public class SiteUUID extends ExtendedUUID implements SiteAddress {
    protected String site, name;

    public SiteUUID() {
    }

    public SiteUUID(long mostSigBits, long leastSigBits, String name, String site) {
        super(mostSigBits,leastSigBits);
        this.name=name;
        this.site=site;
    }

    /** @deprecated Use {@link SiteUUID#SiteUUID(long, long, java.lang.String, java.lang.String)} instead */
    @Deprecated(since="5.2.15")
    public SiteUUID(long mostSigBits, long leastSigBits, byte[] name, byte[] site) {
        this(mostSigBits, leastSigBits, name != null? new String(name) : null,
             site != null? new String(site) : null);
    }

    public SiteUUID(UUID uuid, String name, String site) {
        this(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), name, site);
    }

    public String getName() {return name;}
    public String getSite() {return site;}

    public Supplier<? extends UUID> create() {
        return SiteUUID::new;
    }

    public UUID copy() {
        return new SiteUUID(mostSigBits, leastSigBits, name, site);
    }

    @Override
    public String toString() {
        return print(false);
    }

    public int hashCode() {
        int retval=super.hashCode();
        return site != null? site.hashCode() + retval : retval;
    }

    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof SiteUUID))
            return false;
        return compareTo((SiteUUID)obj) == 0;
    }

    @Override
    public int compareTo(Address other) {
        if(other instanceof SiteUUID) {
            String other_site=((SiteUUID)other).getSite();
            if(this.site != null && other_site != null) {
                int rc=this.site.compareTo(other_site);
                //compareTo will check the bits.
                return rc == 0? super.compareTo(other) : rc;
            }
        }
        return super.compareTo(other);
    }

    public String print(boolean detailed) {
        String retval=name != null? name : NameCache.get(this);
        return String.format("%s:%s%s", retval, site != null? site : "<all sites>", detailed? printOthers() : "");
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Util.size(site) + Util.size(name);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeString(site, out);
        Bits.writeString(name, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        super.readFrom(in);
        site=Bits.readString(in);
        name=Bits.readString(in);
    }

    protected String printOthers() {
        StringBuilder sb=new StringBuilder();
        if(flags != 0)
            sb.append(" flags=" + flags + " (" + flags + ")");
        if(keys == null)
            return sb.toString();
        for(int i=0; i < keys.length; i++) {
            byte[] key=keys[i];
            if(key == null)
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
