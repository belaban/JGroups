// $Id: View.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;




/**
 * A view is a local representation of the current membership of a group
 * Only one view is installed in a channel at a time
 * Views contain the address of its creator, an ID and a list of member addresses
 * These adresses are ordered, and the first address is always the coordinator of the view
 * This way, each member of the group knows who the new coordinator will be if the current one
 * crashes or leaves the group.
 * The views are sent between members using the VIEW_CHANGE event.
 */
public class View implements Externalizable, Cloneable
{
    /* A view is uniquely identified by its ViewID
     * The view id contains the creator address and a Lamport time.
     * The Lamport time is the highest timestamp seen or sent from a view.
     * if a view change comes in with a lower Lamport time, the event is discarded.
     */
    protected ViewId      vid = null;

    /**
     * A list containing all the members of the view
     * This list is always ordered, with the coordinator being the first member.
     * the second member will be the new coordinator if the current one disappears
     * or leaves the group.
     */
    protected Vector  members = null;



    /**
     * creates an empty view, should not be used
     */
    public View() {}


    /**
     * Creates a new view
     * @param vid     The view id of this view (can not be null)
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(ViewId vid, Vector members)
    {
        this.vid = vid;
        this.members = members;
    }


    /**
     * Creates a new view
     * @param creator The creator of this view (can not be null)
     * @param id      The lamport timestamp of this view
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(Address creator, long id, Vector members)
    {
        this( new ViewId(creator, id), members);
    }



    /**
     * returns the view ID of this view
     * if this view was created with the empty constructur, null will be returned
     * @return the view ID of this view
     */
    public ViewId  getVid()
    {
        return vid;
    }

    /**
     * returns the creator of this view
     * if this view was created with the empty constructur, null will be returned
     * @return the creator of this view in form of an Address object
     */
    public Address getCreator()
    {
        return vid != null ? vid.getCoordAddress() : null;
    }

    /**
     * Returns a reference to the List of members (ordered)
     * Do NOT change this list, hence your will invalidate the view
     * Make a copy if you have to modify it.
     * @return a reference to the ordered list of members in this view
     */
    public Vector  getMembers()
    {
        return members;
    }

    /**
     * returns true, if this view contains a certain member
     * @param   mbr - the address of the member,
     * @return true if this view contains the member, false if it doesn't
     * if the argument mbr is null, this operation returns false
     */
    public boolean containsMember( Address mbr )
    {
        if ( mbr == null || members == null )
        {
            return false;
        }
        return members.contains( mbr );
    }

    /**
     * returns the number of members in this view
     * @return the number of members in this view 0..n
     */
    public int     size()
    {
        return members == null? 0 : members.size();
    }



    /**
     * creates a copy of this view
     * @return a copy of this view
     */
    public Object clone()
    {
	ViewId vid2=vid != null? (ViewId)vid.clone() : null;
	Vector members2=members != null? (Vector)members.clone() : null;
        return new View(vid2, members2);
    }



    /**
     * debug only
     */
    public String printDetails()
    {
        StringBuffer ret=new StringBuffer();
        ret.append(vid).append("\n\t");
        if ( members != null )
        {
            for (int i=0; i<members.size(); i++)
            {
                ret.append(members.elementAt(i)).append("\n\t");
            }
            ret.append("\n");
        }
        return ret.toString();
    }



    public String toString()
    {
        StringBuffer ret=new StringBuffer();
        ret.append(vid + " " + members);
        return ret.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
	out.writeObject(vid);
	out.writeObject(members);
    }
    
    
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	vid=(ViewId)in.readObject();
	members=(Vector)in.readObject();
    }



}
