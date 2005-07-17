// $Id: MergeView.java,v 1.3 2005/07/17 11:38:05 chrislott Exp $


package org.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
    protected Vector subgroups=null; // subgroups that merged into this single view (a list of Views)


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
    public MergeView(ViewId vid, Vector members, Vector subgroups) {
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
    public MergeView(Address creator, long id, Vector members, Vector subgroups) {
        super(creator, id, members);
        this.subgroups=subgroups;
    }


    public Vector getSubgroups() {
        return subgroups;
    }


    /**
     * creates a copy of this view
     *
     * @return a copy of this view
     */
    public Object clone() {
        ViewId vid2=vid != null ? (ViewId)vid.clone() : null;
        Vector members2=members != null ? (Vector)members.clone() : null;
        Vector subgroups2=subgroups != null ? (Vector)subgroups.clone() : null;
        return new MergeView(vid2, members2, subgroups2);
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append("MergeView::" + super.toString());
        sb.append(", subgroups=" + subgroups);
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(subgroups);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        subgroups=(Vector)in.readObject();
    }


}
