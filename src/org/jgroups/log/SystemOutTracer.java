// $Id: SystemOutTracer.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


/**
 * Provides output to <code>System.out</code>. All methods defined
 * here have permissions that are either protected or package.
 */
public class SystemOutTracer extends SystemTracer {

    SystemOutTracer(int level) {
	super(level);
    }
    
    protected void doPrint(String message) {
	System.out.print(message);
    }
    
    protected void doFlush() {
	System.out.flush();
    }

}
