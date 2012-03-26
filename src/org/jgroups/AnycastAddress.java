package org.jgroups;

import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**
 * This type of address represents a subset of the cluster members in which the total order properties must be applied,
 * e.g. if the cluster membership is {A,B,C,D,E}, an AnycastAddress could be {D,E}.
 * 
 * @author Pedro Ruivo
 * @since 3.1
 */
public class AnycastAddress implements Address {
    protected Collection<Address> destinations = new ArrayList<Address>(5);
    private static final long     serialVersionUID=-3133792315497822421L;

    public AnycastAddress() {
    }

    public AnycastAddress(Collection<Address> addresses) {
        addAll(addresses);
    }

    public AnycastAddress(Address ... addresses) {
        add(addresses);
    }

    public void add(Address ... addresses) {
        if(addresses == null || addresses.length == 0) return;
        for(Address address: addresses)
            _add(address);
    }

    protected void _add(Address address) {
        if(!destinations.contains(address))
            destinations.add(address);
    }

    public void addAll(Collection<Address> addresses) {
        if(addresses != null && !addresses.isEmpty())
            for(Address address: addresses)
                _add(address);
    }

    public Collection<Address> getAddresses() {
        return destinations;
    }

    public int size() {
        int size = Global.INT_SIZE;
        for(Address address : destinations) {
            size += Util.size(address);
        }
        return size;
    }

    @Override
    public String toString() {
        return "AnycastAddress " + destinations;
    }

    @Override
    public int hashCode() {
        int hc = 0;
        for(Address address : destinations) {
            hc += address.hashCode();
        }
        return hc;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(!(obj instanceof AnycastAddress)) {
            return false;
        }

        AnycastAddress other = (AnycastAddress) obj;

        return other == this || (this.destinations.containsAll(other.destinations) &&
                other.destinations.containsAll(this.destinations));
    }

    public int compareTo(Address o) {
        int hc1, hc2;

        if(this == o) return 0;
        if(!(o instanceof AnycastAddress))
            throw new ClassCastException("comparison between different classes: the other object is " +
                    (o != null? o.getClass() : o));
        AnycastAddress other = (AnycastAddress) o;

        hc1 = this.hashCode();
        hc2 = other.hashCode();

        if(hc1 == hc2) {
            return this.destinations.size() < other.destinations.size() ? -1 :
                    this.destinations.size() > other.destinations.size() ? 1 : 0;
        } else {
            return hc1 < hc2 ? -1 : 1;
        }
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddresses(destinations, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        destinations = (Collection<Address>) Util.readAddresses(in, ArrayList.class);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        try {
            writeTo(objectOutput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        try {
            readFrom(objectInput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}