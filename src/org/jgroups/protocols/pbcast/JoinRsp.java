// $Id: JoinRsp.java,v 1.2 2004/03/30 06:47:18 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.View;

import java.io.Serializable;





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
