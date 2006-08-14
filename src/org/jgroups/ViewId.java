// $Id: ViewId.java,v 1.11 2006/08/14 16:05:09 belaban Exp $

package org.jgroups;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;


/**
 * ViewIds are used for ordering views (each view has a ViewId and a list of members).
 * Ordering between views is important for example in a virtual synchrony protocol where
 * all views seen by a member have to be ordered.
 */
public class ViewId implements Externalizable, Comparable, Cloneable, Streamable {
    Address coord_addr=null;   // Address of the issuer of this view
    long id=0;                 // Lamport time of the view


    public ViewId() { // used for externalization
    }


    /**
     * Creates a ViewID with the coordinator address and a Lamport timestamp of 0.
     *
     * @param coord_addr the address of the member that issued this view
     */
    public ViewId(Address coord_addr) {
        this.coord_addr=coord_addr;
    }

    /**
     * Creates a ViewID with the coordinator address and the given Lamport timestamp.
     *
     * @param coord_addr - the address of the member that issued this view
     * @param id         - the Lamport timestamp of the view
     */
    public ViewId(Address coord_addr, long id) {
        this.coord_addr=coord_addr;
        this.id=id;
    }

    /**
     * returns the lamport time of the view
     *
     * @return the lamport time timestamp
     */
    public long getId() {
        return id;
    }


    /**
     * returns the address of the member that issued this view
     *
     * @return the Address of the the issuer
     */
    public Address getCoordAddress() {
        return coord_addr;
    }


    public String toString() {
        return "[" + coord_addr + '|' + id + ']';
    }

    /**
     * Cloneable interface
     * Returns a new ViewID object containing the same address and lamport timestamp as this view
     */
    public Object clone() {
        return new ViewId(coord_addr, id);
    }

    /**
     * Old Copy method, deprecated because it is substituted by clone()
     */
    public ViewId copy() {
        return (ViewId)clone();
    }

    /**
     * Establishes an order between 2 ViewIds. First compare on id. <em>Compare on coord_addr
     * only if necessary</em> (i.e. ids are equal) !
     *
     * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
     */
    public int compareTo(Object other) {
        if(other == null) return 1; //+++ Maybe necessary to throw an exception

        if(!(other instanceof ViewId)) {
            throw new ClassCastException("ViewId.compareTo(): view id is not comparable with different Objects");
        }
        return id > ((ViewId)other).id ? 1 : id < ((ViewId)other).id ? -1 : 0;
    }

    /**
     * Old Compare
     */
    public int compare(Object o) {
        return compareTo(o);
    }


    public boolean equals(Object other_view) {
        return compareTo(other_view) == 0;
    }


    public int hashCode() {
        return (int)id;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(coord_addr);
        out.writeLong(id);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        coord_addr=(Address)in.readObject();
        id=in.readLong();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddress(coord_addr, out);
        out.writeLong(id);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        coord_addr=Util.readAddress(in);
        id=in.readLong();
    }

    public int serializedSize() {
        int retval=Global.LONG_SIZE; // for the id
        retval+=Util.size(coord_addr);
        return retval;
    }

}
