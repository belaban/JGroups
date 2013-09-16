

package org.jgroups;

import org.jgroups.annotations.Immutable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Collections;
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
@Immutable
public class MergeView extends View {
    protected View[] subgroups; // subgroups that merged into this single view (a list of Views)

    public MergeView() { // Used by externalization
    }


   /**
    * Creates a new merge view
    * 
    * @param view_id The view id of this view (can not be null)
    * @param members Contains a list of all the members in the view, can be empty but not null.
    * @param subgroups A list of Views representing the former subgroups
    */
    public MergeView(ViewId view_id, List<Address> members, List<View> subgroups) {
        super(view_id, members);
        this.subgroups=listToArray(subgroups);
    }

    public MergeView(ViewId view_id, Address[] members, List<View> subgroups) {
        super(view_id, members);
        this.subgroups=listToArray(subgroups);
    }


   /**
    * Creates a new view
    * 
    * @param creator The creator of this view (can not be null)
    * @param id The lamport timestamp of this view
    * @param members Contains a list of all the members in the view, can be empty but not null.
    * @param subgroups A list of Views representing the former subgroups
    */
    public MergeView(Address creator, long id, List<Address> members, List<View> subgroups) {
        super(creator, id, members);
        this.subgroups=listToArray(subgroups);
    }


    public List<View> getSubgroups() {
        return Collections.unmodifiableList(Arrays.asList(subgroups));
    }


    public MergeView copy() {
        return this;
    }

    
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("MergeView::").append(super.toString());
        if(subgroups != null && subgroups.length > 0) {
            sb.append(", subgroups=");
            sb.append(Util.printListWithDelimiter(subgroups, ", ", Util.MAX_LIST_PRINT_SIZE));
        }
        return sb.toString();
    }


    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);

        // write subgroups
        int len=subgroups != null? subgroups.length : 0;
        out.writeShort(len);
        if(len == 0)
            return;
        for(View v: subgroups) {
            int index=get(v.getCreator());
            out.writeShort(index);
            out.writeLong(v.getViewId().getId());

            int num_mbrs=v.size();
            out.writeShort(num_mbrs);
            for(Address mbr: v)
                out.writeShort(get(mbr));
        }
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        short len=in.readShort();
        if(len > 0) {
            subgroups=new View[len];
            for(int i=0; i < len; i++) {
                int index=in.readShort();
                long id=in.readLong();
                Address creator=get(index);
                Address[] mbrs=new Address[in.readShort()];
                for(int j=0; j < mbrs.length; j++)
                    mbrs[j]=get(in.readShort());
                View view=View.create(creator, id, mbrs);
                subgroups[i]=view;
            }
        }
    }

    public int serializedSize() {
        int retval=super.serializedSize();
        retval+=Global.SHORT_SIZE; // for size of subgroups vector
        if(subgroups == null)
            return retval;
        for(View v: subgroups) {
            retval+=Global.SHORT_SIZE + Global.LONG_SIZE // creator and ID (ViewId)
              + Global.SHORT_SIZE; // number of members in the subview
            retval+=v.size() * Global.SHORT_SIZE; // a short (index) for each member
        }
        return retval;
    }

    protected static View[] listToArray(List<View> list) {
        if(list == null)
            return null;
        View[] retval=new View[list.size()];
        int index=0;
        for(View view: list)
            retval[index++]=view;
        return retval;
    }

    protected int get(Address member) {
        if(member == null)
            return -1;
        for(int i=0; i < members.length; i++)
            if(member.equals(members[i]))
                return i;
        return -1;
    }

    protected Address get(int index) {
        return members != null && index >= 0 && index < members.length? members[index] : null;
    }

}
