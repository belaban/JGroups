package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.protocols.relay.SiteUUID;

import java.io.*;

/**
 * Address with a cluster name. Used by TP.Bundler.
 * @author Bela Ban
 */
public class SingletonAddress implements Address {
    private static final long serialVersionUID=4251279268511779867L;
    protected final byte[]    cluster_name;
    protected final Address   addr;


    public SingletonAddress(byte[] cluster_name, Address addr) {
        this.cluster_name=cluster_name;
        this.addr=addr;
        if(cluster_name == null)
            throw new NullPointerException("cluster_name must not be null");
    }

    public SingletonAddress() {
        cluster_name=null;
        addr=null;
    }

    public Address getAddress() {
        return addr;
    }

    public byte[] getClusterName() {
        return cluster_name;
    }

    public int size() {
        return 0;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void writeTo(DataOutput out) throws Exception {
        throw new UnsupportedOperationException();
    }

    public void readFrom(DataInput in) throws Exception {
        throw new UnsupportedOperationException();
    }

    public int hashCode() {
        int retval=hash();
        if(addr != null)
            retval+=addr.hashCode();
        return retval;
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof Address))
            throw new IllegalArgumentException("argument is " + obj.getClass());
        return compareTo((Address)obj) == 0;
    }

    public int compareTo(Address o) {
        SingletonAddress other=(SingletonAddress)o;
        if(this == other)
            return 0;
        if(other == null)
            return 1;
        int rc=compareTo(cluster_name, other.cluster_name);
        if(rc != 0)
            return rc;
        if(addr == null && other.addr == null)
            return 0;
        if(addr == null && other.addr != null)
            return -1;
        if(addr != null && other.addr == null)
            return 1;

        if (addr instanceof SiteUUID && !(other.addr instanceof SiteUUID)) {
            return 1;
        } else if (!(addr instanceof SiteUUID) &&  other.addr instanceof SiteUUID) {
            return -1;
        }

        assert addr != null; // this is here to make the (incorrect) 'addr' NPE warning below disappear !
        return addr.compareTo(other.addr);
    }

    public String toString() {
        return new AsciiString(cluster_name) + (addr != null? ":" + addr.toString() : "");
    }

    protected int hash() {
        int h=0;
        for(int i=0; i < cluster_name.length; i++)
            h=31 * h + cluster_name[i];
        return h;
    }

    protected static int compareTo(byte[] v1, byte[] v2) {
        if(v2 == null) return 1;
        if(v1.hashCode() == v2.hashCode())
            return 0;

        int len1=v1.length;
        int len2=v2.length;
        int lim=Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
            byte c1 =v1[k];
            byte c2 =v2[k];
            if (c1 != c2)
                return c1 > c2? 1 : -1;
            k++;
        }
        return len1 > len2? 1 : len1 < len2? -1 : 0;
    }

}
