// $Id: SystemTracer.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


/**
 * Abstract superclass of <code>SystemOutTracer</code> and
 * <code>SystemErrTracer</code>; avoids ever closing output stream.
 */
public abstract class SystemTracer extends Tracer {
    
    /**
     * Constructor.
     *
     * @param level a trace level.
     */
    SystemTracer(int level) {
	super("", level);
    }
    
    /**
     * Flushes any pending output (by calling <code>flush</code>) but does not
     * close the output stream (System.err or System.out).
     * <p>
     * After this method has been called, all calls to <code>print</code>
     * will still be successful.
     */
    public void close() {
	flush();
    }
    
    protected void doClose() {
	// Do nothing
    }
    
}
