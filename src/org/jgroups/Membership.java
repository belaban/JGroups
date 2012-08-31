
package org.jgroups;


import java.util.*;

/**
 * Represents a membership of a cluster group. Class Membership is not exposed to clients and is
 * used by JGroups internally. The membership object holds a list of Address objects that are in the
 * same membership. Each unique address can only exist once.
 *
 * @since 2.0
 * @author Bela Ban
 */
public class Membership {
    /* private vector to hold all the addresses */
    private final List<Address> members=new LinkedList<Address>();


   /**
    * Creates a member ship object with zero members
    */
    public Membership() {
    }


   /**
    * Creates a Membership with a given initial members. The Address references are copied out of
    * the vector, so that the vector passed in as parameters is not the same reference as the vector
    * that the membership class is using
    *
    * @param initial_members
    *           - a list of members that belong to this membership
    */
    public Membership(Collection<Address> initial_members) {
        if(initial_members != null)
            add(initial_members);
    }



   /**
    * Returns a copy (clone) of the members in this membership. The vector returned is immutable in
    * reference to this object. ie, modifying the vector that is being returned in this method will
    * not modify this membership object.
    *
    * @return a list of members
    */
    public List<Address> getMembers() {
        /*clone so that this objects members can not be manipulated from the outside*/
        synchronized(members) {
            return new ArrayList<Address>(members);
        }
    }

   /**
    * Adds a new member to this membership. If the member already exist (Address.equals(Object)
    * returns true then the member will not be added to the membership
    */
    public void add(Address new_member) {
        synchronized(members) {
            if(new_member != null && !members.contains(new_member)) {
                members.add(new_member);
            }
        }
    }

    public void add(Address ... mbrs) {
        for(Address mbr: mbrs)
            add(mbr);
    }

   /**
    * Adds a list of members to this membership
    *
    * @param v
    *           - a vector containing Address objects
    * @throws ClassCastException
    *            if v contains objects that don't implement the Address interface
    *
    */
    public final void add(Collection<Address> v) {
        if(v != null)
            for(Address addr: v)
                add(addr);
    }


   /**
    * Removes an member from the membership. If this member doesn't exist, no action will be
    * performed on the existing membership
    *
    * @param old_member
    *           - the member to be removed
    */
    public void remove(Address old_member) {
        if(old_member != null) {
            synchronized(members) {
                members.remove(old_member);
            }
        }
    }


   /**
    * Removes all the members contained in v from this membership
    *
    * @param v a list of all the members to be removed
    */
    public void remove(Collection<Address> v) {
        if(v != null) {
            synchronized(members) {
                members.removeAll(v);
            }
        }
    }


    public void retainAll(Collection<Address> v) {
        if(v != null) {
            synchronized(members) {
                members.retainAll(v);
            }
        }
    }


    /**
     * Removes all the members from this membership
     */
    public void clear() {
        synchronized(members) {
            members.clear();
        }
    }

   /**
    * Clears the membership and adds all members of v This method will clear out all the old members
    * of this membership by invoking the <code>Clear</code> method. Then it will add all the all
    * members provided in the vector v
    *
    * @param v
    *           - a vector containing all the members this membership will contain
    */
    public void set(Collection<Address> v) {
        clear();
        add(v);
    }


   /**
    * Clears the membership and adds all members of a given membership parameter. Prior to setting
    * membership this method will clear out all the old members of this membership by invoking the
    * <code>clear</code> method.
    *
    * @param m
    *           - a membership containing all the members this membership will contain
    */
    public void set(Membership m) {
        clear();
        if(m != null)
            add(m.getMembers());
    }

   /**
    * Rearranges the members so that the specified address is the first in the list, and therefore will
    * act as coordinator.
    *
    * @param coord
    *          - the address that should become coordinator
    * @return true on success, else false.
    */
    public boolean setCoordinator(Address coord) {
      if(coord == null) return false;
      synchronized(members) {
          if (!members.contains(coord))
              return false;

          members.remove(coord);
          members.add(0, coord);
      }
      return true;
    }


    /**
     * Merges membership with the new members and removes suspects.
     * The Merge method will remove all the suspects and add in the new members.
     * It will do it in the order
     * 1. Remove suspects
     * 2. Add new members
     * the order is very important to notice.
     *
     * @param new_mems - a vector containing a list of members (Address) to be added to this membership
     * @param suspects - a vector containing a list of members (Address) to be removed from this membership
     */
    public void merge(Collection<Address> new_mems, Collection<Address> suspects) {
        remove(suspects);
        add(new_mems);
    }


   /**
    * Returns true if the provided member belongs to this membership
    *
    * @param member
    * @return true if the member belongs to this membership
    */
    public boolean contains(Address member) {
        if(member == null) return false;
        synchronized(members) {
            return members.contains(member);
        }
    }


    public void sort() {
        synchronized(members) {
            Collections.sort(members);
        }
    }




    /**
     * Returns a copy of this membership
     *
     * @return an exact copy of this membership
     */
    public Membership copy() {
        return new Membership(this.members);
    }


   /**
    * Returns the number of addresses in this membership
    *
    * @return the number of addresses in this membership
    */
    public int size() {
        synchronized(members) {
            return members.size();
        }
    }

   /**
    * Returns the component at the specified index
    *
    * @param index
    *           - 0..size()-1
    * @throws ArrayIndexOutOfBoundsException
    *            - if the index is negative or not less than the current size of this Membership
    *            object.
    */
    public Address elementAt(int index) {
        synchronized(members) {
            return members.get(index);
        }
    }


    public String toString() {
        synchronized(members) {
            return members.toString();
        }
    }


}
