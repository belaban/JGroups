package org.jgroups.jmx.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.protocols.*;

/**
 * @author Bela Ban
 * @version $Id: STATS.java,v 1.1 2005/06/07 10:17:26 belaban Exp $
 */
public class STATS extends org.jgroups.jmx.Protocol implements STATSMBean {
    org.jgroups.protocols.STATS p;

    public STATS() {
    }

    public STATS(Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.STATS)p;
    }

    public void attachProtocol(Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.STATS)p;
    }

    public long getSentMessages() {
        return p.getSentMessages();
    }

    public long getSentBytes() {
        return p.getSentBytes();
    }

    public long getSentUnicastMessages() {
        return p.getSentUnicastMessages();
    }

    public long getSentUnicastBytes() {
        return p.getSentUnicastBytes();
    }

    public long getSentMcastMessages() {
        return p.getSentMcastMessages();
    }

    public long getSentMcastBytes() {
        return p.getSentMcastBytes();
    }

    public long getReceivedMessages() {
        return p.getReceivedMessages();
    }

    public long getReceivedBytes() {
        return p.getReceivedBytes();
    }

    public long getReceivedUnicastMessages() {
        return p.getReceivedUnicastMessages();
    }

    public long getReceivedUnicastBytes() {
        return p.getReceivedUnicastBytes();
    }

    public long getReceivedMcastMessages() {
        return p.getReceivedMcastMessages();
    }

    public long getReceivedMcastBytes() {
        return p.getReceivedMcastBytes();
    }

    public String printStats() {
        return p.printStats();
    }
}
