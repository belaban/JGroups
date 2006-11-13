// $Id: LockingException.java,v 1.2 2006/11/13 17:42:10 bstansberry Exp $


package org.jgroups.blocks;

import java.util.Map;


public class LockingException extends Exception {

	private static final long serialVersionUID = -712594616520011007L;
	
	Map failed_lockers=null; // list of members who failed acquiring locks (keys=Address, values=exception string)

    public LockingException(String msg) {
	super(msg);
    }

    public LockingException(Map m) {
        super("LockingException");
        failed_lockers=m;
    }

    
    public String toString() {
        StringBuffer sb=new StringBuffer();

        sb.append(super.toString());

        if(failed_lockers != null && failed_lockers.size() > 0)
            sb.append(" (failed members: ").append(failed_lockers);
        return sb.toString();
    }
    
}
