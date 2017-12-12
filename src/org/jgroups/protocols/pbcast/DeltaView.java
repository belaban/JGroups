package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.ArrayIterator;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Subclass of {@link org.jgroups.View} with a null members field. Adds an array for left members and one for joined
 * members compared to the previous view. A recipient receiving a DeltaView can construct a new view by grabbing the
 * view corresponding to {@link #view_id}, removing the left members and adding the new members.<p/>
 * This class is only used with VIEW messages in GMS to install new views (not merge views). When a VIEW
 * message is received, the DeltaView is read from the {@link org.jgroups.protocols.pbcast.GMS.GmsHeader}, a View is
 * constructured and the header discarded. Therefore, the lifetime of a DeltaView is short: it is created and set in
 * a GmsHeader, the header is then marshalled. On the receiving side, the DeltaView is created from the stream, a View
 * is created and the DeltaView discarded again.<p/>
 * Instances of this class are created by {@link CoordGmsImpl#handleMembershipChange(java.util.Collection)}.<p/>
 * JIRA issue: https://issues.jboss.org/browse/JGRP-1354
 * @author Bela Ban
 * @since  3.4
 */
public class DeltaView extends View {
    /** The fields left_members and new_members refer to the view corresponding to ref_view_id */
    protected ViewId    ref_view_id;

    /** Members which left the view corresponding to ref_view_id */
    protected Address[] left_members;

    /** Members which joined the view corresponding to ref_view_id */
    protected Address[] new_members;

    public DeltaView() {

    }


    public DeltaView(ViewId view_id, ViewId ref_view_id, Address[] left_members, Address[] new_members) {
        this.view_id=view_id;
        this.ref_view_id=ref_view_id;
        this.left_members=left_members;
        this.new_members=new_members;
        if(view_id == null)      throw new IllegalArgumentException("view_id cannot be null");
        if(ref_view_id == null) throw new IllegalArgumentException("ref_view_id cannot be null");
    }

    public ViewId    getRefViewId()     {return ref_view_id;}
    public Address[] getLeftMembers()   {return left_members;}
    public Address[] getNewMembers()    {return new_members;}

    @Override
    public int serializedSize() {
        int retval=view_id.serializedSize() + ref_view_id.serializedSize();
        retval+=Util.size(left_members);
        retval+=Util.size(new_members);
        return retval;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        view_id.writeTo(out);
        ref_view_id.writeTo(out);
        Util.writeAddresses(left_members, out);
        Util.writeAddresses(new_members, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        view_id=new ViewId();
        view_id.readFrom(in);
        ref_view_id=new ViewId();
        ref_view_id.readFrom(in);
        left_members=Util.readAddresses(in);
        new_members=Util.readAddresses(in);
    }

    public Iterator<Address> iterator() {
        Address[] combined=new Address[left_members.length + new_members.length];
        int left_len=left_members.length;
        System.arraycopy(left_members, 0, combined, 0, left_len);
        System.arraycopy(new_members, 0, combined, left_len, new_members.length);
        return new ArrayIterator<>(combined);
    }

    public String toString() {
        StringBuilder sb=new StringBuilder(super.toString()).append(", ref-view=").append(ref_view_id);
        if(left_members != null && left_members.length > 0)
            sb.append(", left=").append(Arrays.toString(left_members));
        if(new_members != null && new_members.length > 0)
            sb.append(", joined=").append(Arrays.toString(new_members));
        return sb.toString();
    }
}
