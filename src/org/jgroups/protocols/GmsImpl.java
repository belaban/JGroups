// $Id: GmsImpl.java,v 1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;

import java.util.Vector;
import org.jgroups.*;
import org.jgroups.log.Trace;



public abstract class GmsImpl {
    protected GMS          gms=null;


    public abstract void      init(); // initialize variables
    
    public abstract void      join(Address mbr);
    public abstract void      leave(Address mbr);
    public abstract void      suspect(Address mbr);
    public abstract void      merge(Vector coords);

    public abstract boolean   handleJoin(Address mbr);
    public abstract void      handleLeave(Address mbr, boolean suspected);
    public abstract void      handleViewChange(ViewId new_view, Vector mbrs);
    public abstract View      handleMerge(ViewId  other_view,Vector other_members);
    public abstract void      handleSuspect(Address mbr);

    public boolean            handleUpEvent(Event evt) {return true;}
    public boolean            handleDownEvent(Event evt) {return true;}




    protected void wrongMethod(String method_name) {
	Trace.error("GmsImpl.wrongMethod()", "method " + method_name +
		    "() should not be sent to an instance of " + getClass().getName());
    }



    /**
       Returns potential coordinator based on lexicographic ordering of member addresses. Another
       approach would be to keep track of the primary partition and return the first member if we
       are the primary partition.
     */
    protected boolean iWouldBeCoordinator(Vector new_mbrs) {
	Membership tmp_mbrs=gms.members.copy();
	tmp_mbrs.merge(new_mbrs, null);
	tmp_mbrs.sort();
	if(tmp_mbrs.size() <= 0 || gms.local_addr == null)
	    return false;
	return gms.local_addr.equals(tmp_mbrs.elementAt(0));
    }

}
