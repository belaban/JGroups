package org.jgroups.util;

import org.jgroups.Address;

import java.io.*;

/**
 * Address with a cluster name. Used by TP.Bundler.
 * @author Bela Ban
 */
public class SingletonAddress implements Address {
    private static final long serialVersionUID=7295848950180741550L;
    protected final String  cluster_name;
    protected final Address addr;

    public SingletonAddress(String cluster_name, Address addr) {
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

    public String getClusterName() {
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
        int retval=0;
        if(cluster_name != null)
            retval+=cluster_name.hashCode();
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
        int rc=cluster_name.compareTo(other.cluster_name);
        if(rc != 0)
            return rc;
        if(addr == null && other.addr == null)
            return 0;
        if(addr == null && other.addr != null)
            return -1;
        if(addr != null && other.addr == null)
            return 1;

        assert addr != null; // this is here to make the (incorrect) 'addr' NPE warning below disappear !
        return addr.compareTo(other.addr);
    }

    public String toString() {
        return cluster_name + (addr != null? ":" + addr.toString() : "");
    }
}
