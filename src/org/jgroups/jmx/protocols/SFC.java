package org.jgroups.jmx.protocols;

import org.jgroups.Address;
import org.jgroups.jmx.Protocol;

import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: SFC.java,v 1.2.2.2 2007/04/18 03:18:43 bstansberry Exp $
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

    public long getMulticastMaxCredits() {
        return p.getMulticastMaxCredits();
    }

    public long getUnicastMaxCredits() {
        return p.getMulticastMaxCredits();
    }

    public long getMulticastCredits() {
        return p.getMulticastCredits();
    }

    public long getBytesSent() {
        return p.getBytesSent();
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

     public long getCredits(Address peer) {
         return p.getCredits(peer);
     }

     public long getBytesSent(Address peer) {
         return p.getBytesSent(peer);
     }

     public long getBlockings(Address peer) {
         return p.getBlockings(peer);
     }

     public long getCreditRequestsReceived(Address peer) {
         return p.getCreditRequestsReceived(peer);
     }

     public long getCreditRequestsSent(Address peer) {
         return p.getCreditRequestsSent(peer);
     }

     public long getReplenishmentsReceived(Address peer) {
         return p.getReplenishmentsReceived(peer);
     }

     public long getReplenishmentsSent(Address peer) {
         return p.getReplenishmentsSent(peer);
     }

     public long getTotalBlockingTime(Address peer) {
         return p.getTotalBlockingTime(peer);
     }

      public double getAverageBlockingTime(Address peer) {
          return p.getAverageBlockingTime(peer);
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
