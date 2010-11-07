// $Id: JoinRsp.java,v 1.12 2007/04/04 05:23:33 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.View;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.util.Digest;

import java.io.Serializable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;


/**
 * Result of a JOIN request (sent by the GMS client). Instances of this class are immutable.
 */
public class JoinRsp implements Serializable, Streamable {
    private View view=null;
    private Digest digest=null;
    /** only set if JOIN failed, e.g. in AUTH */
    private String fail_reason=null;
    private static final long serialVersionUID = -212620440767943314L;



    public JoinRsp() {

    }

    public JoinRsp(View v, Digest d) {
        view=v;
        digest=d;
    }

    public JoinRsp(String fail_reason) {
        this.fail_reason=fail_reason;
    }

    public View getView() {
        return view;
    }

    public Digest getDigest() {
        return digest;
    }

    public String getFailReason() {
        return fail_reason;
    }

    public void setFailReason(String r) {
        fail_reason=r;
    }


    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeStreamable(view, out);
        Util.writeStreamable(digest, out);
        Util.writeString(fail_reason, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        view=(View)Util.readStreamable(View.class, in);
        digest=(Digest)Util.readStreamable(Digest.class, in);
        fail_reason=Util.readString(in);
    }

    public int serializedSize() {
        int retval=Global.BYTE_SIZE * 2; // presence for view and digest
        if(view != null)
            retval+=view.serializedSize();
        if(digest != null)
            retval+=digest.serializedSize();

        retval+=Global.BYTE_SIZE; // presence byte for fail_reason
        if(fail_reason != null)
            retval+=fail_reason.length() +2;
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("view: ");
        if(view == null)
            sb.append("<null>");
        else
            sb.append(view);
        sb.append(", digest: ");
        if(digest == null)
            sb.append("<null>");
        else
            sb.append(digest);
        if(fail_reason != null)
            sb.append(", fail reason: ").append(fail_reason);
        return sb.toString();
    }
}
