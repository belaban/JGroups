
package org.jgroups;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;


/**
 * ViewIds are used for ordering views (each view has a ViewId and a list of members).
 * Ordering between views is important for example in a virtual synchrony protocol where
 * all views seen by a member have to be ordered.
 */
public class ViewId implements Comparable<ViewId>, SizeStreamable, Constructable<ViewId> {
    protected Address creator;   // Address of the creator of this view
    protected long    id;        // Lamport time of the view


    public ViewId() { // used for externalization
    }


    /**
     * Creates a ViewID with the coordinator address and a Lamport timestamp of 0.
     *
     * @param creator the address of the member that issued this view
     */
    public ViewId(Address creator) {
        this.creator=creator;
        if(this.creator == null)
            throw new IllegalArgumentException("creator cannot be null");
    }

    /**
     * Creates a ViewID with the coordinator address and the given Lamport timestamp.
     *
     * @param creator - the address of the member that issued this view
     * @param id         - the Lamport timestamp of the view
     */
    public ViewId(Address creator, long id) {
        this(creator);
        this.id=id;
    }

    public Supplier<? extends ViewId> create() {
        return ViewId::new;
    }

    /**
     * Returns the address of the member that issued this view
     *
     * @return the Address of the the creator
     */
    public Address getCreator() {
        return creator;
    }

    /**
     * returns the lamport time of the view
     *
     * @return the lamport time timestamp
     */
    public long getId() {
        return id;
    }

    public String toString() {
        return "[" + creator + '|' + id + ']';
    }


    public ViewId copy() {
        return new ViewId(creator, id);
    }

    /**
     * Establishes an order between 2 ViewIds. The comparison is done on the IDs, if they are equal, we use the creator.
     *
     * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
     */
    public int compareTo(ViewId other) {
        return id > other.id ? 1 : id < other.id ? -1 : creator.compareTo(other.creator);
    }

    /**
     * Establishes an order between 2 ViewIds. <em>Note that we compare only on IDs !</em>
     *
     * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
     */
    public int compareToIDs(ViewId other) {
        return id > other.id ? 1 : id < other.id ? -1 : 0;
    }


    public boolean equals(Object other) {
        return other instanceof ViewId && (this == other  || compareTo((ViewId)other) == 0);
    }


    public int hashCode() {
        return (int)(creator.hashCode() + id);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Util.writeAddress(creator, out);
        Bits.writeLong(id,out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        creator=Util.readAddress(in);
        id=Bits.readLong(in);
    }

    @Override
    public int serializedSize() {
        return Bits.size(id) + Util.size(creator);
    }

}
