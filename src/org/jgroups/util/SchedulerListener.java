// $Id: SchedulerListener.java,v 1.2 2005/07/17 11:33:58 chrislott Exp $

package org.jgroups.util;

/**
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
