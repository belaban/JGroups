package org.jgroups.protocols.relay;

import org.jgroups.util.*;

import java.io.*;

/**
 * Implementation of SiteAddress
 * @author Bela Ban
 * @since 3.2
 */
public class SiteUUID extends UUID implements SiteAddress {
    private static final long serialVersionUID=-8602137120498053578L;
    protected String name; // logical name, can be null
    protected String site; // site name


    public SiteUUID() {
    }


    public SiteUUID(long mostSigBits, long leastSigBits, String name, String site) {
        super(mostSigBits,leastSigBits);
        this.name=name;
        this.site=site;
    }

    public SiteUUID(UUID uuid, String name, String site) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.name=name;
        this.site=site;
    }

    public String getName() {
        return name;
    }

    public String getSite() {
        return site;
    }

    public UUID copy() {
        return new SiteUUID(mostSigBits, leastSigBits, name, site);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        name=in.readUTF();
        site=in.readUTF();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(name);
        out.writeUTF(site);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        name=Bits.readString(in);
        site=Bits.readString(in);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Bits.writeString(name,out);
        Bits.writeString(site,out);
    }

    public int size() {
        return super.size() + Util.size(name) + Util.size(site);
    }


    public String toString() {
        String retval=name != null? name : super.toString();
        return retval + ":" + site;
    }

}
