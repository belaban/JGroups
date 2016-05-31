
package org.jgroups;


import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**
 * A view is a local representation of the current membership of a group. Only one view is installed
 * in a channel at a time. Views contain the address of its creator, an ID and a list of member
 * addresses. These addresses are ordered, and the first address is always the coordinator of the
 * view. This way, each member of the group knows who the new coordinator will be if the current one
 * crashes or leaves the group. The views are sent between members using the VIEW_CHANGE event
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class View implements Comparable<View>, Streamable, Iterable<Address> {

   /**
    * A view is uniquely identified by its ViewID. The view id contains the creator address and a
    * Lamport time. The Lamport time is the highest timestamp seen or sent from a view. if a view
    * change comes in with a lower Lamport time, the event is discarded.
    */
    protected ViewId vid;

   /**
    * A list containing all the members of the view.This list is always ordered, with the
    * coordinator being the first member. the second member will be the new coordinator if the
    * current one disappears or leaves the group.
    */
    protected List<Address> members;



    /**
     * Creates an empty view, should not be used, only used by (de-)serialization
     */
    public View() {
    }


    /**
     * Creates a new view
     *
     * @param vid     The view id of this view (can not be null)
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(ViewId vid, List<Address> members) {
        this.vid=vid;
        this.members=new ArrayList<Address>(members);
    }

    /**
     * Creates a new view
     *
     * @param creator The creator of this view (can not be null)
     * @param id      The lamport timestamp of this view
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(Address creator, long id, List<Address> members) {
        this(new ViewId(creator, id), members);
    }


    public static View create(Address coord, long id, Address ... members) {
        return new View(new ViewId(coord, id), Arrays.asList(members));
    }

    /**
     * Returns the view ID of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the view ID of this view
     */
    public ViewId getVid()    {return vid;}
    public ViewId getViewId() {return vid;}

    /**
     * Returns the creator of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the creator of this view in form of an Address object
     */
    public Address getCreator() {
        return vid.getCreator();
    }

    public Address getCoord() {return !members.isEmpty()? members.get(0) : null;}

    /**
     * Returns a reference to the List of members (ordered)
     * Do NOT change this list, hence your will invalidate the view
     * Make a copy if you have to modify it.
     *
     * @return a reference to the ordered list of members in this view
     */
    public List<Address> getMembers() {
        return Collections.unmodifiableList(members);
    }

    /** Returns the underlying list. The caller <em>must not</em> modify the contents. Should not be used by
     *  application code ! This method may be removed at any time, so don't use it !
     */
    public List<Address> getMembersRaw() {
           return members;
       }



    /**
     * Returns true, if this view contains a certain member
     *
     * @param mbr - the address of the member,
     * @return true if this view contains the member, false if it doesn't
     *         if the argument mbr is null, this operation returns false
     */
    public boolean containsMember(Address mbr) {
        return mbr != null && members.contains(mbr);
    }

    /** Returns true if all mbrs are elements of this view, false otherwise */
    public boolean containsMembers(Address ... mbrs) {
        if(mbrs == null || members == null)
            return false;
        for(Address mbr: mbrs) {
            if(!containsMember(mbr))
                return false;
        }
        return true;
    }

    public boolean containsMembers(Collection<Address> mbrs) {
        if(mbrs == null || members == null)
            return false;
        for(Address mbr: mbrs) {
            if(!containsMember(mbr))
                return false;
        }
        return true;
    }


    public int compareTo(View o) {
        return vid.compareTo(o.vid);
    }

    public boolean equals(Object obj) {
        return obj instanceof View && (this == obj || compareTo((View)obj) == 0);
    }


    public int hashCode() {
        return vid.hashCode();
    }

    /**
     * Returns the number of members in this view
     *
     * @return the number of members in this view 0..n
     */
    public int size() {
        return members.size();
    }


    public View copy() {
        return new View(vid.copy(), members);
    }



    public String toString() {
        StringBuilder sb=new StringBuilder(64);
        sb.append(vid).append(" ");
        if(members != null)
            sb.append("[").append(Util.printListWithDelimiter(members, ", ", Util.MAX_LIST_PRINT_SIZE)).append("]");
        return sb.toString();
    }



    public void writeTo(DataOutput out) throws Exception {
        vid.writeTo(out);
        Util.writeAddresses(members, out);
    }

    @SuppressWarnings("unchecked") 
    public void readFrom(DataInput in) throws Exception {
        vid=new ViewId();
        vid.readFrom(in);
        members=(List<Address>)Util.readAddresses(in, ArrayList.class);
    }

    public int serializedSize() {
        return (int)(vid.serializedSize() + Util.size(members));
    }


    public Iterator<Address> iterator() {
        return members.iterator();
    }
}
