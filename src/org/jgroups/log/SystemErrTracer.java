// $Id: SystemErrTracer.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


/**
 * Provides output to <code>System.err</code>. All methods defined
 * here have permissions that are either protected or package.
 */
public class SystemErrTracer extends SystemTracer {

    SystemErrTracer(int level) {
	super(level);
    }
    
    protected void doPrint(String message) {
	System.err.print(message);
    }
    
    protected void doFlush() {
	System.err.flush();
    }

}
