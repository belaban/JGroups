

package org.jgroups;

import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A view that is sent as a result of a cluster merge. Whenever a group splits into subgroups, e.g., due to
 * a network partition, and later the subgroups merge back together, a MergeView instead of a View
 * will be received by the application. The MergeView class is a subclass of View and contains as
 * additional instance variable: the list of views that were merged. For example, if the group
 * denoted by view V1:(p,q,r,s,t) splits into subgroups V2:(p,q,r) and V2:(s,t), the merged view
 * might be V3:(p,q,r,s,t). In this case the MergeView would contain a list of 2 views: V2:(p,q,r)
 * and V2:(s,t).
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class MergeView extends View {
    protected List<View> subgroups=null; // subgroups that merged into this single view (a list of Views)


    /**
     * Used by externalization
     */
    public MergeView() {
    }


   /**
    * Creates a new view
    * 
    * @param vid
    *           The view id of this view (can not be null)
    * @param members
    *           Contains a list of all the members in the view, can be empty but not null.
    * @param subgroups
    *           A list of Views representing the former subgroups
    */
    public MergeView(ViewId vid, List<Address> members, List<View> subgroups) {
        super(vid, members);
        this.subgroups=subgroups;
    }


   /**
    * Creates a new view
    * 
    * @param creator
    *           The creator of this view (can not be null)
    * @param id
    *           The lamport timestamp of this view
    * @param members
    *           Contains a list of all the members in the view, can be empty but not null.
    * @param subgroups
    *           A list of Views representing the former subgroups
    */
    public MergeView(Address creator, long id, List<Address> members, List<View> subgroups) {
        super(creator, id, members);
        this.subgroups=subgroups;
    }


    public List<View> getSubgroups() {
        return subgroups;
    }


    public View copy() {
        ViewId vid2=vid.copy();
        List<Address> members2=members != null ? new ArrayList<Address>(members) : null;
        List<View> subgroups2=subgroups != null ? new ArrayList<View>(subgroups) : null;
        return new MergeView(vid2, members2, subgroups2);
    }

    
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("MergeView::").append(super.toString());
        if(subgroups != null && !subgroups.isEmpty()) {
            sb.append(", subgroups=");
            sb.append(Util.printListWithDelimiter(subgroups, ", ", Util.MAX_LIST_PRINT_SIZE));
        }
        return sb.toString();
    }


    public void writeTo(DataOutput out) throws Exception {
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

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        short len=in.readShort();
        if(len > 0) {
            View v;
            subgroups=new ArrayList<View>(len);
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
