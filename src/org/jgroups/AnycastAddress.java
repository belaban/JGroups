package org.jgroups;

import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This type of address represents a subset of the cluster members in which the total order properties must be applied,
 * e.g. if the cluster membership is {A,B,C,D,E}, an AnycastAddress could be {D,E}.
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class AnycastAddress implements Address {
    protected Collection<Address> destinations;
    private static final long serialVersionUID = -3133792315497822421L;

    public AnycastAddress() {
    }

    public AnycastAddress(Collection<Address> addresses) {
        addAll(addresses);
    }

    public AnycastAddress(Address... addresses) {
        add(addresses);
    }

    public void add(Address... addresses) {
        if (addresses.length == 0) {
            return;
        }
        initCollection(addresses.length);
        for (Address address : addresses) {
            internalAdd(address);
        }
    }

    protected void internalAdd(Address address) {
        if (!destinations.contains(address))
            destinations.add(address);
    }

    public void addAll(Collection<Address> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            return;
        }
        initCollection(addresses.size());
        for (Address address : addresses) {
            internalAdd(address);
        }
    }

    public Collection<Address> getAddresses() {
        return destinations;
    }

    private void initCollection(int estimatedSize) {
        if (destinations == null) {
            destinations = new ArrayList<>(estimatedSize);
        }
    }

    public int size() {
        if (destinations == null) {
            return Global.INT_SIZE;
        }
        int size = Global.INT_SIZE;
        for (Address address : destinations) {
            size += Util.size(address);
        }
        return size;
    }

    @Override
    public String toString() {
        return "AnycastAddress " + destinations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AnycastAddress that = (AnycastAddress) o;

        return !(destinations != null ? !destinations.equals(that.destinations) : that.destinations != null);

    }

    @Override
    public int hashCode() {
        return destinations != null ? destinations.hashCode() : 0;
    }

    public int compareTo(Address o) {
        int hc1, hc2;

        if (this == o) return 0;
        if (!(o instanceof AnycastAddress))
            throw new ClassCastException("comparison between different classes: the other object is " +
                    (o != null ? o.getClass() : o));
        AnycastAddress other = (AnycastAddress) o;

        hc1 = this.hashCode();
        hc2 = other.hashCode();

        if (hc1 == hc2) {
            int size = destinations == null ? 0 : destinations.size();
            int otherSize = other.destinations == null ? 0 : other.destinations.size();
            //it is always positive and they should be small. safe to do this:
            return size - otherSize;
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
