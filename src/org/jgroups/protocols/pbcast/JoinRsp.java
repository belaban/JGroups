// $Id: JoinRsp.java,v 1.4 2004/10/08 12:19:10 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.View;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.Serializable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;


public class JoinRsp implements Serializable, Streamable {
    View view=null;
    Digest digest=null;

    public JoinRsp(View v, Digest d) {
        view=v;
        digest=d;
    }


    View getView() {
        return view;
    }

    Digest getDigest() {
        return digest;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeStreamable(view, out);
        Util.writeStreamable(digest, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        view=(View)Util.readStreamable(View.class, in);
        digest=(Digest)Util.readStreamable(Digest.class, in);
    }

    public String toString() {
        StringBuffer sb=new StringBuffer();
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
        return sb.toString();
    }
}
