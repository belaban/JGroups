// $Id: SchedulerListener.java,v 1.2.10.1 2007/03/08 10:23:20 belaban Exp $

package org.jgroups.util;

/**
 * Provides callback for use with a {@link Scheduler}.
 */
public interface SchedulerListener {
	/**
	 * @param rt
	 * @param r
	 */
    void started(ReusableThread rt, Runnable   r);
	/**
	 * @param rt
	 * @param r
	 */
    void stopped(ReusableThread rt, Runnable   r);
	/**
	 * @param rt
	 * @param r
	 */
    void suspended(ReusableThread rt, Runnable r);
	/**
	 * @param rt
	 * @param r
	 */
    void resumed(ReusableThread rt, Runnable   r);
}
