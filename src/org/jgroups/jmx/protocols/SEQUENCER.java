package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: SEQUENCER.java,v 1.1 2006/01/03 14:43:43 belaban Exp $
 */
public class SEQUENCER extends Protocol implements SEQUENCERMBean {
    org.jgroups.protocols.SEQUENCER p;

    public SEQUENCER() {
    }

    public SEQUENCER(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.SEQUENCER)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.SEQUENCER)p;
    }

    public boolean isCoord() {
        return p.isCoordinator();
    }

    public String getCoordinator() {
        return p.getCoordinator().toString();
    }

    public String getLocalAddress() {
        return p.getLocalAddress().toString();
    }

    public long getForwarded() {
        return p.getForwarded();
    }

    public long getBroadcast() {
        return p.getBroadcast();
    }

    public long getReceivedForwards() {
        return p.getReceivedForwards();
    }

    public long getReceivedBroadcasts() {
        return p.getReceivedBroadcasts();
    }

    public void resetStats() {
        super.resetStats();
    }

    public String printStats() {
        return super.printStats();
    }

    public Map dumpStats() {
        return super.dumpStats();
    }
}
