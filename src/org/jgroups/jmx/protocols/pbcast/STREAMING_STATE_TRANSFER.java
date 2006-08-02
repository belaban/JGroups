package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.Protocol;

/**
 * @author Vladimir Blagojevic
 * @version $Id$
 */
public class STREAMING_STATE_TRANSFER extends Protocol implements STREAMING_STATE_TRANSFERMBean {
    org.jgroups.protocols.pbcast.STREAMING_STATE_TRANSFER p;

    public STREAMING_STATE_TRANSFER() {
    }

    public STREAMING_STATE_TRANSFER(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.pbcast.STREAMING_STATE_TRANSFER)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.pbcast.STREAMING_STATE_TRANSFER)p;
    }

    public int getNumberOfStateRequests() {
        return p.getNumberOfStateRequests();
    }

    public long getNumberOfStateBytesSent() {
        return p.getNumberOfStateBytesSent();
    }

    public double getAverageStateSize() {
        return p.getAverageStateSize();
    }	
}
