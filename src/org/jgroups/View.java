
package org.jgroups;


import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.Collection;


/**
 * A view is a local representation of the current membership of a group.
 * Only one view is installed in a channel at a time.
 * Views contain the address of its creator, an ID and a list of member addresses.
 * These adresses are ordered, and the first address is always the coordinator of the view.
 * This way, each member of the group knows who the new coordinator will be if the current one
 * crashes or leaves the group.
 * The views are sent between members using the VIEW_CHANGE event
 * @author Bela Ban
 * @version $Id: View.java,v 1.22 2009/06/12 09:57:13 belaban Exp $
 */
public class View implements Externalizable, Cloneable, Streamable {
    /* A view is uniquely identified by its ViewID
     * The view id contains the creator address and a Lamport time.
     * The Lamport time is the highest timestamp seen or sent from a view.
     * if a view change comes in with a lower Lamport time, the event is discarded.
     */
    protected ViewId vid=null;

    /**
     * A list containing all the members of the view
     * This list is always ordered, with the coordinator being the first member.
     * the second member will be the new coordinator if the current one disappears
     * or leaves the group.
     */
    protected Vector<Address> members=null;
    
    protected Map<String, Object> payload=null;
    private static final long serialVersionUID=7027860705519930293L;


    /**
     * creates an empty view, should not be used
     */
    public View() {
    }


    /**
     * Creates a new view
     *
     * @param vid     The view id of this view (can not be null)
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(ViewId vid, Vector<Address> members) {
        this.vid=vid;
        this.members=members;
    }

    public View(ViewId vid, Collection<Address> members) {
        this.vid=vid;
        this.members=new Vector<Address>(members);
    }

    /**
     * Creates a new view
     *
     * @param creator The creator of this view (can not be null)
     * @param id      The lamport timestamp of this view
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(Address creator, long id, Vector<Address> members) {
        this(new ViewId(creator, id), members);
    }


    /**
     * returns the view ID of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the view ID of this view
     */
    public ViewId getVid() {
        return vid;
    }

    public ViewId getViewId() {return vid;}

    /**
     * returns the creator of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the creator of this view in form of an Address object
     */
    public Address getCreator() {
        return vid != null ? vid.getCoordAddress() : null;
    }

    /**
     * Returns a reference to the List of members (ordered)
     * Do NOT change this list, hence your will invalidate the view
     * Make a copy if you have to modify it.
     *
     * @return a reference to the ordered list of members in this view
     */
    public Vector<Address> getMembers() {
        return Util.unmodifiableVector(members);
    }

    /**
     * returns true, if this view contains a certain member
     *
     * @param mbr - the address of the member,
     * @return true if this view contains the member, false if it doesn't
     *         if the argument mbr is null, this operation returns false
     */
    public boolean containsMember(Address mbr) {
        return !(mbr == null || members == null) && members.contains(mbr);
    }


    public boolean equals(Object obj) {
        if(!(obj instanceof View))
            return false;
        if(vid != null) {
            int rc=vid.compareTo(((View)obj).vid);
            if(rc != 0)
                return false;
            if(members != null && ((View)obj).members != null) {
                return members.equals(((View)obj).members);
            }
        }
        else {
            if(((View)obj).vid == null)
                return true;
        }
        return false;
    }


    public int hashCode() {
        return vid != null? vid.hashCode() : 0;
    }

    /**
     * returns the number of members in this view
     *
     * @return the number of members in this view 0..n
     */
    public int size() {
        return members == null ? 0 : members.size();
    }


    /**
     * creates a copy of this view
     *
     * @return a copy of this view
     */
    public Object clone() {
        ViewId vid2=vid != null ? (ViewId)vid.clone() : null;
        Vector<Address> members2=members != null ? new Vector<Address>(members) : null;
        return new View(vid2, members2);
    }


    /**
     * debug only
     */
    public String printDetails() {
        StringBuilder ret=new StringBuilder();
        ret.append(vid).append("\n\t");
        if(members != null) {
            for(int i=0; i < members.size(); i++) {
                ret.append(members.elementAt(i)).append("\n\t");
            }
            ret.append('\n');
        }
        return ret.toString();
    }

    /**
     * Adds a key and value to the view. Since the payloads will be shipped around *with* the view, so the keys and
     * values need to be serializable. Note that the total serialized size of <em>all</em> keys and values cannot
     * exceed 65000 bytes !
     * @param key
     * @param value
     */
    public void addPayload(String key, Object value) {
        if(payload == null) {
            payload=new HashMap<String, Object>(7);
        }
        payload.put(key, value);
    }

    public Object removePayload(String key) {
        return payload != null? payload.remove(key) : null;
    }

    public Object getPayload(String key) {
        if(payload != null)
            return payload.get(key);
        return null;
    }


    public String toString() {
        StringBuilder ret=new StringBuilder(64);
        ret.append(vid).append(" ").append(members);
        return ret.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vid);
        out.writeObject(members);
        if(payload != null && !payload.isEmpty()) {
            out.writeBoolean(true);
            out.writeObject(payload);
        }
        else {
            out.writeBoolean(false);
        }
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vid=(ViewId)in.readObject();
        members=(Vector<Address>)in.readObject();
        if(in.readBoolean()) {
            payload=(Map<String, Object>)in.readObject();
        }
    }


    public void writeTo(DataOutputStream out) throws IOException {
        // vid
        if(vid != null) {
            out.writeBoolean(true);
            vid.writeTo(out);
        }
        else
            out.writeBoolean(false);

        // members:
        Util.writeAddresses(members, out);

        if(payload != null && !payload.isEmpty()) {
            try {
                byte buffer[]=Util.objectToByteBuffer(payload);
                out.writeShort(buffer.length);
                out.write(buffer, 0, buffer.length);
            }
            catch(Exception e) {
                throw new IOException("could not write View payload");
            }
        }
        else {
            out.writeShort(0);
        }
    }


    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        boolean b;
        // vid:
        b=in.readBoolean();
        if(b) {
            vid=new ViewId();
            vid.readFrom(in);
        }

        // members:
        members=(Vector<Address>)Util.readAddresses(in, Vector.class);

        short payloadLength=in.readShort();
        if(payloadLength > 0) {
            byte[] buffer=new byte[payloadLength];
            in.read(buffer);
            try {
                payload=(Map<String, Object>)Util.objectFromByteBuffer(buffer);
            }
            catch(Exception e) {
                throw new IOException("Could not read View payload " + buffer.length);
            }
        }
    }

    public int serializedSize() {
        int retval=Global.BYTE_SIZE; // presence for vid
        if(vid != null)
            retval+=vid.serializedSize();
        retval+=Util.size(members);

        retval+=Global.SHORT_SIZE; // presence for payload
        if(payload != null) {
            retval+=Util.sizeOf(payload);
        }
        return retval;
    }


}
