// $Id: Digest.java,v 1.3 2004/09/23 16:29:41 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.util.List;
import org.jgroups.util.Util;

import java.io.Serializable;




/**
 * Message digest, collecting the highest sequence number seen so far for each member, plus the
 * messages that have higher seqnos than the ones given.
 */
public class Digest implements Serializable {
    public long[]     highest_seqnos=null; // highest seqno received for each member
    public final List       msgs=new List();     // msgs (for each member) whose seqnos are higher than the
                                           // ones sent by the FLUSH coordinator
    public Digest(int size) {
	highest_seqnos=new long[size];
    }

    public String toString() {
	StringBuffer retval=new StringBuffer();
	retval.append(Util.array2String(highest_seqnos) + " (" + msgs.size() + " msgs)");
	return retval.toString();
    }
    
}
