// $Id: Digest.java,v 1.2 2004/03/30 06:47:21 belaban Exp $

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
    public List       msgs=new List();     // msgs (for each member) whose seqnos are higher than the 
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
