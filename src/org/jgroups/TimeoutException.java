// $Id: TimeoutException.java,v 1.2 2005/01/07 22:44:46 ovidiuf Exp $

package org.jgroups;

import java.util.List;



public class TimeoutException extends Exception {
    List failed_mbrs=null; // members that failed responding

    public TimeoutException() {
        super("TimeoutException");
    }

    public TimeoutException(String msg) {
        super(msg);
    }

    public TimeoutException(List failed_mbrs) {
        super("TimeoutException");
        this.failed_mbrs=failed_mbrs;
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();

        sb.append(super.toString());

        if(failed_mbrs != null && failed_mbrs.size() > 0)
            sb.append(" (failed members: ").append(failed_mbrs);
        return sb.toString();
    }
}
