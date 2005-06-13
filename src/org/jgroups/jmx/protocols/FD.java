package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: FD.java,v 1.1 2005/06/13 12:44:06 belaban Exp $
 */
public class FD extends Protocol implements FDMBean {
    org.jgroups.protocols.FD p;

    public FD() {
    }

    public FD(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.FD)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.FD)p;
    }

    public int getNumberOfHeartbeatsSent() {
        return p.getNumberOfHeartbeatsSent();
    }

    public int getNumSuspectEventsGenerated() {
        return p.getNumSuspectEventsGenerated();
    }

    public long getTimeout() {
        return p.getTimeout();
    }

    public void setTimeout(long timeout) {
        p.setTimeout(timeout);
    }

    public int getMaxTries() {
        return p.getMaxTries();
    }

    public void setMaxTries(int max_tries) {
        p.setMaxTries(max_tries);
    }

    public int getCurrentNumTries() {
        return p.getCurrentNumTries();
    }

    public boolean isShun() {
        return p.isShun();
    }

    public void setShun(boolean flag) {
        p.setShun(flag);
    }

    public String getLocalAddress() {
        return p.getLocalAddress();
    }

    public String getMembers() {
        return p.getMembers();
    }

    public String getPingableMembers() {
        return p.getPingableMembers();
    }

    public String getPingDest() {
        return p.getPingDest();
    }

    public String printSuspectHistory() {
        return p.printSuspectHistory();
    }
}
