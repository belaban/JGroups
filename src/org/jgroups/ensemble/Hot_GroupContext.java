// $Id: Hot_GroupContext.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

import java.util.Vector;


public class Hot_GroupContext {
    private static int idCount = 0;
    private static Vector allGctx = new Vector();
    public int id;
    public Hot_GroupContext next;
    public Hot_Callbacks conf;
    public Object env;
    public boolean group_blocked;
    public boolean joining;
    public boolean leaving;
	
    private Hot_GroupContext() {
	id = 0;
	group_blocked = false;
	joining = false;
	leaving = false;
    }

    public synchronized static Hot_GroupContext alloc() {
	Hot_GroupContext gctx = new Hot_GroupContext();
	gctx.id = idCount++;
	allGctx.addElement(gctx);
	return gctx;	
    }
	
    public synchronized static void release(Hot_GroupContext gc) {
	// delete this one from the Vector
	allGctx.removeElement(gc);
    }
	
    public synchronized static Hot_GroupContext lookup(int id) {
	// should use a hash table rather than a vector, but....
	Hot_GroupContext gc = null;
	for (int i = 0;i < allGctx.size();i++) {
	    gc = (Hot_GroupContext)allGctx.elementAt(i);
	    if (gc.id == id) {
		return gc;
 	    }
	}
	Hot_Ensemble.panic("lookup_gctx: id not found!!");
	return null;
    }


    public String toString() {
	StringBuffer ret=new StringBuffer();
	ret.append("id=" + id + ", group_blocked=" + group_blocked + 
		   ", joining=" + joining + ", leaving=" + leaving); 
	return ret.toString();
    }

}
