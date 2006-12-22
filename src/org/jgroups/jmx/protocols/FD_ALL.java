package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: FD_ALL.java,v 1.1 2006/12/22 09:34:53 belaban Exp $
 */
public class FD_ALL extends Protocol implements FD_ALLMBean {
    org.jgroups.protocols.FD_ALL p;

    public FD_ALL() {
    }

    public FD_ALL(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.FD_ALL)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.FD_ALL)p;
    }

    public int getHeartbeatsSent() {
        return p.getHeartbeatsSent();
    }

    public int getHeartbeatsReceived() {
        return p.getHeartbeatsReceived();
    }

    public int getSuspectEventsSent() {
        return p.getSuspectEventsSent();
    }

    public long getTimeout() {
        return p.getTimeout();
    }

    public void setTimeout(long timeout) {
        p.setTimeout(timeout);
    }

    public long getInterval() {
        return p.getInterval();
    }

    public void setInterval(long interval) {
        p.setInterval(interval);
    }

    public boolean isShun() {
        return p.isShun();
    }

    public void setShun(boolean flag) {
        p.setShun(flag);
    }

    public boolean isRunning() {
        return p.isRunning();
    }

    public String getLocalAddress() {
        return p.getLocalAddress();
    }

    public String getMembers() {
        return p.getMembers();
    }

    public String printSuspectHistory() {
        return p.printSuspectHistory();
    }

    public String printTimestamps() {
        return p.printTimestamps();
    }
}
