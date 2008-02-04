// $Id: SchedulerListener.java,v 1.3 2008/02/04 13:43:14 belaban Exp $

package org.jgroups.util;

/**
 * @deprecated This class will be removed in 3.0
 * Provides callback for use with a {@link Scheduler}.
 */
public interface SchedulerListener {
	/**
	 * @param r
	 */
    void started(Runnable   r);
    /**
     * @param r
     */
    void stopped(Runnable   r);
    /**
     * @param r
     */
    void suspended(Runnable r);
    /**
     * @param r
     */
    void resumed(Runnable   r);
}
