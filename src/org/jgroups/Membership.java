// $Id: Membership.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;


import java.util.Vector;
import org.jgroups.log.Trace;


/**
  * Class to keep track of Addresses.
  * The member ship object holds a vector of Address object that are in the same membership
  * Each unique address can only exist once, ie, doing Membership.add(existing_address) will be ignored
  *
  *
  */
public class Membership implements Cloneable {
    /* private vector to hold all the addresses */
    private Vector members = null;

    /**
     * Public constructor
     * Creates a member ship object with zero members
     */
    public Membership()
    {
        // 11 is the optimized value for vector growth
        members=new Vector(11);
    }


    /**
     * Creates a member ship object with the initial members.
     * The Address references are copied out of the vector, so that the
     * vector passed in as parameters is not the same reference as the vector
     * that the membership class is using
     * @param   initial_members - a list of members that belong to this membership
     */
    public Membership(Vector initial_members)
    {
        if(initial_members != null)
            members = (Vector)initial_members.clone();
    }


    /**
     * returns a copy (clone) of the members in this membership.
     * the vector returned is immutable in reference to this object.
     * ie, modifying the vector that is being returned in this method
     * will not modify this membership object.
     * @return a list of members,
     */
    public Vector getMembers()
    {
        /*clone so that this objects members can not be manipulated from the outside*/
        return (Vector)members.clone();
    }


    /**
     * Adds a new member to this membership.
     * If the member already exist (Address.equals(Object) returns true then the member will
     * not be added to the membership
     */
    public synchronized void add(Address new_member)
    {
        if(new_member != null && !members.contains(new_member))
        {
            members.addElement(new_member);
        }
    }


    /**
     * Adds a list of members to this membership
     * @param v - a vector containing Address objects
     * @see Add
     * @exception ClassCastException if v contains objects that don't implement the Address interface
     */
    public synchronized void add(Vector v)
    {
        if(v != null)
        {
            for(int i=0; i < v.size(); i++)
            {
                add((Address)v.elementAt(i));
            }
        }
    }


    /**
     * removes an member from the membership.
     * If this member doesn't exist, no action will be performed on the existing membership
     * @param old_member - the member to be removed
     */
    public synchronized void remove(Address old_member)
    {
        if(old_member != null)
        {
            members.remove(old_member);
        }
    }


    /**
     * removes all the members contained in v from this membership
     * @param v - a vector containing all the members to be removed
     */
    public synchronized void remove(Vector v)
    {
        if(v != null)
        {
            members.removeAll(v);
        }
    }


    /**
     * removes all the members from this membership
     */
    public synchronized void clear()
    {
        members.removeAllElements();
    }

    /**
     * Clear the membership and adds all members of v
     * This method will clear out all the old members of this membership by
     * invoking the <code>Clear</code> method.
     * Then it will add all the all members provided in the vector v
     * @param   v - a vector containing all the members this membership will contain
     */
    public synchronized void set(Vector v)
    {
        clear();
        if(v != null)
        {
            add(v);
        }
    }



    /**
     * Clear the membership and adds all members of v
     * This method will clear out all the old members of this membership by
     * invoking the <code>Clear</code> method.
     * Then it will add all the all members provided in the vector v
     * @param   m - a membership containing all the members this membership will contain
     */
    public synchronized void set(Membership m)
    {
        clear();
        if(m != null)
        {
            add( m.getMembers() );
        }
    }


    /**
     * merges membership with the new members and removes suspects
     * The Merge method will remove all the suspects and add in the new members.
     * It will do it in the order
     * 1. Remove suspects
     * 2. Add new members
     * the order is very important to notice.
     * @param   new_mems - a vector containing a list of members (Address) to be added to this membership
     * @param   suspects - a vector containing a list of members (Address) to be removed from this membership
     */
    public synchronized void merge(Vector new_mems, Vector suspects)
    {
        remove(suspects);
        add(new_mems);
    }


    /**
     * Returns true if the provided member belongs to this membership
     * @param   member
     * @return true if the member belongs to this membership
     */
    public synchronized boolean contains(Address member)
    {
        if(member == null) return false;
        return members.contains(member);
    }



    /* Simple inefficient bubble sort, but not used very often (only when merging) */
    public synchronized void sort()
    {
        Address  a1;
        Address  a2;
        Address  tmp;

        for(int j=0; j < members.size(); j++)
        {
            for(int i=0; i < members.size()-1; i++)
            {
                a1=(Address)members.elementAt(i);
                a2=(Address)members.elementAt(i+1);
                if(a1 == null || a2 == null)
                {
                    Trace.error("Membership.sort()", "member's address is null");
                    continue;
                }
                if(a1.compareTo(a2) > 0)
                {
                    tmp=a2;
                    members.setElementAt(a1, i+1);
                    members.setElementAt(tmp, i);
                }
            }
        }
    }


    /**
     * returns a copy of this membership
     * @return an exact copy of this membership
     */
    public synchronized Membership copy()
    {
        return((Membership)clone());
    }


    /**
     * @return a clone of this object. The list of members is copied to a new
     * container
     */
    public synchronized Object clone() {
        Membership m;

        try {
	    m = (Membership)super.clone();
	    m.members = (Vector)members.clone();
	    return(m);
        }
	catch(CloneNotSupportedException ex) {
	    throw new InternalError();
        }
    }


    /**
     * Returns the number of addresses in this membership
     * @return the number of addresses in this membership
     */
    public synchronized int size()
    {
        return members.size();
    }

    /**
     * Returns the component at the specified index
     * @param index - 0..size()-1
     * @exception ArrayIndexOutOfBoundsException - if the index is negative or not less than the current size of this Membership object.
     * @see java.util.Vector.elementAt
     */

    public synchronized Object elementAt(int index)
    {
        return members.elementAt(index);
    }


    public synchronized String toString()
    {
        return members.toString();
    }


}
