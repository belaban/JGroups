package org.jgroups.groups;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**
 * This type of address represents a group in which the total order properties must be applied
 * 
 * @author Pedro Ruivo
 * @since 3.1
 */
public class GroupAddress implements Address {
    private Set<Address> destinations;

    public GroupAddress() {
        destinations = new HashSet<Address>();
    }

    public void addAddress(Address address) {
        destinations.add(address);
    }

    public void addAllAddress(Collection<Address> addresses) {
        destinations.addAll(addresses);
    }

    public Set<Address> getAddresses() {
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
        return "GroupAddress{" + destinations + "}";
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
        if(!(obj instanceof GroupAddress)) {
            return false;
        }

        GroupAddress other = (GroupAddress) obj;

        return other == this || (this.destinations.containsAll(other.destinations) &&
                other.destinations.containsAll(this.destinations));
    }

    public int compareTo(Address o) {
        int hc1, hc2;

        if(this == o) return 0;
        if(!(o instanceof GroupAddress))
            throw new ClassCastException("comparison between different classes: the other object is " +
                    (o != null? o.getClass() : o));
        GroupAddress other = (GroupAddress) o;

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
        destinations = (Set<Address>) Util.readAddresses(in, HashSet.class);
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