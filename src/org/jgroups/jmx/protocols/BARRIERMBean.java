package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * JMX interface for BARRIER protocol.
 * 
 * @author rpike
 */
public interface BARRIERMBean extends ProtocolMBean {

	/** Indicates if barrier is currently closed. */
	public boolean isClosed();
	
	/** Gets configured max_close_time value (ms). */
	public long getMaxCloseTime();

    /** Returns true if <tt>barrier_opener_future</tt> is non-null. */
	public boolean isOpenerScheduled();
	
	/**
	 * Returns the current count of in-flight threads.
	 * <p>In-flight threads are those currently processing in higher-level protocols.
	 * 
	 * @return in-flight threads count
	 */
	public int getInFlightThreadsCount();
}
