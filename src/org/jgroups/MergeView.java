// $Id: MergeView.java,v 1.8 2007/03/12 10:51:46 belaban Exp $


package org.jgroups;

import java.io.*;
import java.util.Vector;


/**
 * A view that is sent as a result of a merge.
 * Whenever a group splits into subgroups, e.g., due to a network partition, 
 * and later the subgroups merge back together, a MergeView instead of a View 
 * will be received by the application. The MergeView class is a subclass of 
 * View and contains as additional instance variable: the list of views that 
 * were merged. For example, if the group denoted by view V1:(p,q,r,s,t) 
 * splits into subgroups V2:(p,q,r) and V2:(s,t), the merged view might be 
 * V3:(p,q,r,s,t). In this case the MergeView would contain a list of 2 views: 
 * V2:(p,q,r) and V2:(s,t).
 */
public class MergeView extends View {
    protected Vector<View> subgroups=null; // subgroups that merged into this single view (a list of Views)


    /**
     * Used by externalization
     */
    public MergeView() {
    }


    /**
     * Creates a new view
     *
     * @param vid       The view id of this view (can not be null)
     * @param members   Contains a list of all the members in the view, can be empty but not null.
     * @param subgroups A list of Views representing the former subgroups
     */
    public MergeView(ViewId vid, Vector<Address> members, Vector<View> subgroups) {
        super(vid, members);
        this.subgroups=subgroups;
    }


    /**
     * Creates a new view
     *
     * @param creator   The creator of this view (can not be null)
     * @param id        The lamport timestamp of this view
     * @param members   Contains a list of all the members in the view, can be empty but not null.
     * @param subgroups A list of Views representing the former subgroups
     */
    public MergeView(Address creator, long id, Vector<Address> members, Vector<View> subgroups) {
        super(creator, id, members);
        this.subgroups=subgroups;
    }


    public Vector<View> getSubgroups() {
        return subgroups;
    }


    /**
     * creates a copy of this view
     *
     * @return a copy of this view
     */
    public Object clone() {
        ViewId vid2=vid != null ? (ViewId)vid.clone() : null;
        Vector<Address> members2=members != null ? (Vector<Address>)members.clone() : null;
        Vector<View> subgroups2=subgroups != null ? (Vector<View>)subgroups.clone() : null;
        return new MergeView(vid2, members2, subgroups2);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("MergeView::").append(super.toString()).append(", subgroups=").append(subgroups);
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(subgroups);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        subgroups=(Vector<View>)in.readObject();
    }


    public void writeTo(DataOutputStream out) throws IOException {
        super.writeTo(out);

        // write subgroups
        int len=subgroups != null? subgroups.size() : 0;
        out.writeShort(len);
        if(len == 0)
            return;
        for(View v: subgroups) {
            if(v instanceof MergeView)
                out.writeBoolean(true);
            else
                out.writeBoolean(false);
            v.writeTo(out);
        }
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        super.readFrom(in);
        short len=in.readShort();
        if(len > 0) {
            View v;
            subgroups=new Vector<View>();
            for(int i=0; i < len; i++) {
                boolean is_merge_view=in.readBoolean();
                v=is_merge_view? new MergeView() : new View();
                v.readFrom(in);
                subgroups.add(v);
            }
        }
    }

    public int serializedSize() {
        int retval=super.serializedSize();
        retval+=Global.SHORT_SIZE; // for size of subgroups vector

        if(subgroups == null)
            return retval;
        for(View v: subgroups) {
            retval+=Global.BYTE_SIZE; // boolean for View or MergeView
            retval+=v.serializedSize();
        }
        return retval;
    }


}
