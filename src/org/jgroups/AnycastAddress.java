package org.jgroups;

import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * This type of address represents a subset of the cluster members in which the total order properties must be applied,
 * e.g. if the cluster membership is {A,B,C,D,E}, an AnycastAddress could be {D,E}.
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class AnycastAddress implements Address, Constructable<AnycastAddress> {
    protected Collection<Address> destinations;

    public AnycastAddress() {
    }

    public AnycastAddress(Collection<Address> addresses) {
        addAll(addresses);
    }

    public AnycastAddress(Address... addresses) {
        add(addresses);
    }

    public Supplier<? extends AnycastAddress> create() {
        return AnycastAddress::new;
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
        addresses.forEach(this::internalAdd);
    }

    public Collection<Address> getAddresses() {
        return destinations;
    }

    public Optional<Collection<Address>> findAddresses() {
        return Optional.ofNullable(destinations);
    }

    private void initCollection(int estimatedSize) {
        if (destinations == null) {
            destinations = new ArrayList<>(estimatedSize);
        }
    }

    public int serializedSize() {
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
        return Objects.equals(destinations, that.destinations);
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
        destinations=Util.readAddresses(in, ArrayList::new);
    }

}
