// $Id: JoinRsp.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols.pbcast;


import java.io.Serializable;
import org.jgroups.*;





public class JoinRsp implements Serializable {
    View    view=null;
    Digest  digest=null;

    public JoinRsp(View v, Digest d) {
	view=v;
	digest=d;
    }


    View   getView()   {return view;}
    Digest getDigest() {return digest;}


    public String toString() {
	StringBuffer sb=new StringBuffer();
	sb.append("view: ");
	if(view == null) sb.append("<null>");
	else sb.append(view);
	sb.append(", digest: ");
	if(digest == null) sb.append("<null>");
	else sb.append(digest);
	return sb.toString();
    }
}
