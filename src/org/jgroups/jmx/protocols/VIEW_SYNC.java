package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: VIEW_SYNC.java,v 1.2.14.1 2008/03/12 09:01:42 belaban Exp $
 */
public class VIEW_SYNC extends Protocol implements VIEW_SYNCMBean {
    org.jgroups.protocols.VIEW_SYNC p;

    public VIEW_SYNC() {
    }

    public VIEW_SYNC(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.VIEW_SYNC)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.VIEW_SYNC)p;
    }

    public long getAverageSendInterval() {
        return p.getAverageSendInterval();
    }

    public void setAverageSendInterval(long send_interval) {
        p.setAverageSendInterval(send_interval);
    }

    public int getNumViewsSent() {
        return p.getNumViewsSent();
    }

    public int getNumViewsAdjusted() {
        return p.getNumViewsAdjusted();
    }

	public long getLastViewRequestSent() {
		return p.getLastViewRequestSent();
	}

	public int getNumViewRequestsSent() {
		return p.getNumViewRequestsSent();
	}

	public int getNumViewResponsesSeen() {
		return p.getNumViewRequestsSent();
	}

	public int getNumViewsLess() {
		return p.getNumViewsLess();
	}

	public int getNumViewsEqual() {
		return p.getNumViewsEqual();
	}

	public int getNumViewsNonLocal() {
		return p.getNumViewsNonLocal();
	}

    public void sendViewRequest() {
        p.sendViewRequest();
    }

//    public void sendFakeViewForTestingOnly() {
//        p.sendFakeViewForTestingOnly();
//    }

}
