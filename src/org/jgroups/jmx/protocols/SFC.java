package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: SFC.java,v 1.2 2007/01/09 10:19:23 belaban Exp $
 */
public class SFC extends Protocol implements SFCMBean {
    org.jgroups.protocols.SFC p;

    public SFC() {
    }

    public SFC(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.SFC)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.SFC)p;
    }

    public void resetStats() {
        super.resetStats();
        p.resetStats();
    }

    public long getMaxCredits() {
        return p.getMaxCredits();
    }

    public long getBytesSent() {
        return p.getBytesSent();
    }

    public long getCredits() {
        return p.getCredits();
    }

    public long getBlockings() {
        return p.getBlockings();
    }

    public long getCreditRequestsReceived() {
        return p.getCreditRequestsReceived();
    }

    public long getCreditRequestsSent() {
        return p.getCreditRequestsSent();
    }

    public long getReplenishmentsReceived() {
        return p.getReplenishmentsReceived();
    }

    public long getReplenishmentsSent() {
        return p.getReplenishmentsSent();
    }

    public long getTotalBlockingTime() {
        return p.getTotalBlockingTime();
    }

     public double getAverageBlockingTime() {
         return p.getAverageBlockingTime();
     }

    public Map dumpStats() {
        return p.dumpStats();
    }

    public String printBlockingTimes() {
        return p.printBlockingTimes();
    }

    public String printReceived() {
        return p.printReceived();
    }

    public String printPendingCreditors() {
        return p.printPendingCreditors();
    }

    public String printPendingRequesters() {
        return p.printPendingRequesters();
    }

    public void unblock() {
        p.unblock();
    }
}
