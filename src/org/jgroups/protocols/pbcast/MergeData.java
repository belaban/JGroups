// $Id: MergeData.java,v 1.8 2009/04/27 13:19:09 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.util.Digest;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Encapsulates data sent with a MERGE_RSP (handleMergeResponse()) and INSTALL_MERGE_VIEW
 * (handleMergeView()).
 *
 * @author Bela Ban Oct 22 2001
 */
public class MergeData implements Externalizable {
    Address   sender=null;
    boolean   merge_rejected=false;
    View      view=null;
    Digest    digest=null;

    /**
     * Empty constructor needed for externalization
     */
    public MergeData() {
    }

    public MergeData(Address sender, View view, Digest digest) {
        this.sender=sender;
        this.view=view;
        this.digest=digest;
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

    public void setView(View v) {
        view=v;
    }

    public void setDigest(Digest d) {
        digest=d;
    }


    public boolean equals(Object other) {
        return sender != null && other != null && other instanceof MergeData &&
                ((MergeData)other).sender != null && ((MergeData)other).sender.equals(sender);
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sender);
        out.writeBoolean(merge_rejected);
        if(!merge_rejected) {
            out.writeObject(view);
            out.writeObject(digest);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sender=(Address)in.readObject();
        merge_rejected=in.readBoolean();
        if(!merge_rejected) {
            view=(View)in.readObject();
            digest=(Digest)in.readObject();
        }
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("sender=").append(sender);
        if(merge_rejected)
            sb.append(" (merge_rejected)");
        else {
            sb.append(", view=").append(view).append(", digest=").append(digest);
        }
        return sb.toString();
    }


}



