
package org.jgroups;


import org.jgroups.annotations.Immutable;
import org.jgroups.util.ArrayIterator;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
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
@Immutable
public class View implements Comparable<View>, Streamable, Iterable<Address> {

   /**
    * A view is uniquely identified by its ViewID. The view id contains the creator address and a
    * Lamport time. The Lamport time is the highest timestamp seen or sent from a view. if a view
    * change comes in with a lower Lamport time, the event is discarded.
    */
    protected ViewId    view_id;

   /**
    * An array containing all the members of the view. This array is always ordered, with the
    * coordinator being the first member. The second member will be the new coordinator if the
    * current one disappears or leaves the group.
    */
    protected Address[] members;

    protected static final boolean suppress_view_size=Boolean.getBoolean(Global.SUPPRESS_VIEW_SIZE);


    /**
     * Creates an empty view, should not be used, only used by (de-)serialization
     */
    public View() {
    }


    /**
     * Creates a new view
     *
     * @param view_id The view id of this view (can not be null)
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(ViewId view_id, List<Address> members) {
        this.view_id=view_id;
        if(members == null)
            throw new IllegalArgumentException("members cannot be null");
        this.members=new Address[members.size()];
        int index=0;
        for(Address member: members)
            this.members[index++]=member;
    }

    /**
     * Creates a new view.
     * @param view_id The new view-id
     * @param members The members. Note that the parameter is <em>not</em> copied.
     */
    public View(ViewId view_id, Address[] members) {
        this.view_id=view_id;
        this.members=members;
        if(members == null)
            throw new IllegalArgumentException("members cannot be null");
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
        return new View(new ViewId(coord, id), members);
    }

    @Deprecated public ViewId getVid()    {return view_id;}

    /**
     * Returns the view ID of this view
     * if this view was created with the empty constructur, null will be returned
     * @return the view ID of this view
     */
    public ViewId getViewId() {return view_id;}

    /**
     * Returns the creator of this view
     * if this view was created with the empty constructur, null will be returned
     * @return the creator of this view in form of an Address object
     */
    public Address getCreator() {
        return view_id.getCreator();
    }

    /**
     * Returns the member list
     * @return an unmodifiable list
     */
    public List<Address> getMembers() {
        return Collections.unmodifiableList(Arrays.asList(members));
    }

    /** Returns the underlying array. The caller <em>must not</em> modify the contents. Should not be used by
     *  application code ! This method may be removed at any time, so don't use it !
     */
    public Address[] getMembersRaw() {
        return members;
    }

    /**
     * Returns true if this view contains a certain member
     * @param mbr - the address of the member,
     * @return true if this view contains the member, false if it doesn't
     */
    public boolean containsMember(Address mbr) {
        if(mbr == null || members == null)
            return false;
        for(Address member: members)
            if(member != null && member.equals(mbr))
                return true;
        return false;
    }


    public int compareTo(View o) {
        return view_id.compareTo(o.view_id);
    }

    public boolean equals(Object obj) {
        return this.getClass() == obj.getClass() && (this == obj || compareTo((View)obj) == 0);
    }

    public boolean deepEquals(View other) {
        return this == other || equals(other) && Arrays.equals(members, other.members);
    }


    public int hashCode() {
        return view_id.hashCode();
    }

    /**
     * Returns the number of members in this view
     * @return the number of members in this view 0..n
     */
    public int size() {
        return members.length;
    }

    /**
     * Creates a copy of a view
     * @return
     * @deprecated View is immutable, so copy() is unnecessary
     */
    @Deprecated public View copy() {
        return this;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder(64);
        sb.append(view_id);
        if(members != null) {
            if(!suppress_view_size)
                sb.append(" (").append(members.length).append(")");
            sb.append(" [").append(Util.printListWithDelimiter(members,", ",Util.MAX_LIST_PRINT_SIZE)).append("]");
        }
        return sb.toString();
    }


    public void writeTo(DataOutput out) throws Exception {
        view_id.writeTo(out);
        Util.writeAddresses(members,out);
    }

    @SuppressWarnings("unchecked") 
    public void readFrom(DataInput in) throws Exception {
        view_id=new ViewId();
        view_id.readFrom(in);
        members=Util.readAddresses(in);
    }

    public int serializedSize() {
        return (int)(view_id.serializedSize() + Util.size(members));
    }

    /**
     * Returns a list of members which left from view one to two
     * @param one
     * @param two
     * @return
     */
    public static List<Address> leftMembers(View one, View two) {
        if(one == null || two == null)
            return null;
        List<Address> retval=new ArrayList<>(one.getMembers());
        retval.removeAll(two.getMembers());
        return retval;
    }

    /**
     * Returns the difference between 2 views from and to. It is assumed that view 'from' is logically prior to view 'to'.
     * @param from The first view
     * @param to The second view
     * @return an array of 2 Address arrays: index 0 has the addresses of the joined member, index 1 those of the left members
     */
    public static Address[][] diff(final View from, final View to) {
        if(to == null)
            throw new IllegalArgumentException("the second view cannot be null");
        if(from == null) {
            Address[] joined=new Address[to.size()];
            int index=0;
            for(Address addr: to.getMembers())
                joined[index++]=addr;
            return new Address[][]{joined,{}};
        }

        Address[] joined=null, left=null;
        int num_joiners=0, num_left=0;

        // determine joiners
        for(Address addr: to)
            if(!from.containsMember(addr))
                num_joiners++;
        if(num_joiners > 0) {
            joined=new Address[num_joiners];
            int index=0;
            for(Address addr: to)
                if(!from.containsMember(addr))
                    joined[index++]=addr;
        }

        // determine leavers
        for(Address addr: from)
            if(!to.containsMember(addr))
                num_left++;
        if(num_left > 0) {
            left=new Address[num_left];
            int index=0;
            for(Address addr: from)
                if(!to.containsMember(addr))
                    left[index++]=addr;
        }

        return new Address[][]{joined != null? joined : new Address[]{}, left != null? left : new Address[]{}};
    }

    public Iterator<Address> iterator() {
        return new ArrayIterator(this.members);
    }

}
