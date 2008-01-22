// $Id: LockingException.java,v 1.3 2008/01/22 10:44:31 belaban Exp $


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
        StringBuilder sb=new StringBuilder();

        sb.append(super.toString());

        if(failed_lockers != null && failed_lockers.size() > 0)
            sb.append(" (failed members: ").append(failed_lockers);
        return sb.toString();
    }
    
}

