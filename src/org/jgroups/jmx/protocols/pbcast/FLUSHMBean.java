package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Vladimir Blagojevic
 * @version $Id$
 */
public interface FLUSHMBean extends ProtocolMBean {
	
	public double getAverageFlushDuration();
	
	public long getTotalTimeInFlush();
	
	public int getNumberOfFlushes();
	
    boolean startFlush(long timeout);

    void stopFlush(); 
}
