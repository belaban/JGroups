
package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.util.Digest;


/**
 * Encapsulates data sent with a MERGE_RSP (handleMergeResponse()) and INSTALL_MERGE_VIEW (handleMergeView()).<p/>
 * Note that since MergeData is never sent across the network, it doesn't need to be Streamable.
 * @author Bela Ban Oct 22 2001
 */
public class MergeData  {
    protected final Address   sender;
    protected final boolean   merge_rejected;
    protected final View      view;
    protected final Digest    digest;


    public MergeData(Address sender, View view, Digest digest, boolean merge_rejected) {
        this.sender=sender;
        this.view=view;
        this.digest=digest;
        this.merge_rejected=merge_rejected;
    }

    public MergeData(Address sender, View view, Digest digest) {
        this(sender, view, digest, false);
    }

    public Address getSender() {
        return sender;
    }

    public View getView() {
        return view;
    }

    public Digest getDigest() {
        return digest;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder("sender=").append(sender);
        if(merge_rejected)
            sb.append(" (merge_rejected)");
        else
            sb.append(", view=").append(view).append(", digest=").append(digest);
        return sb.toString();
    }


}



