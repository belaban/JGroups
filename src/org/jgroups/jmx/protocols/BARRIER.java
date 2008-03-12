package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * JMX wrapper for BARRIER protocol.
 * 
 * @author rpike
 */
public class BARRIER extends Protocol implements BARRIERMBean {
	private org.jgroups.protocols.BARRIER p;
	
	public BARRIER() {
	}
	
  public BARRIER(org.jgroups.stack.Protocol p) {
    super(p);
    this.p=(org.jgroups.protocols.BARRIER)p;
  }

	public void attachProtocol(org.jgroups.stack.Protocol p) {
	    super.attachProtocol(p);
	    this.p=(org.jgroups.protocols.BARRIER)p;
	}
	
	public int getInFlightThreadsCount() {
		return p.getNumberOfInFlightThreads();
	}

	public long getMaxCloseTime() {
		return p.getMaxCloseTime();
	}

	public boolean isClosed() {
		return p.isClosed();
	}

	public boolean isOpenerScheduled() {
		return p.isOpenerScheduled();
	}
}
