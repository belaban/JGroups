package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: STABLE.java,v 1.4 2007/01/09 11:40:16 belaban Exp $
 */
public class STABLE extends Protocol implements STABLEMBean {
    org.jgroups.protocols.pbcast.STABLE p;

    public STABLE() {
    }

    public STABLE(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.pbcast.STABLE)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.pbcast.STABLE)p;
    }

    public long getDesiredAverageGossip() {
        return p.getDesiredAverageGossip();
    }

    public void setDesiredAverageGossip(long gossip_interval) {
        p.setDesiredAverageGossip(gossip_interval);
    }

    public long getMaxBytes() {
        return p.getMaxBytes();
    }

    public void setMaxBytes(long max_bytes) {
        p.setMaxBytes(max_bytes);
    }

    public long getBytes() {
        return p.getBytes();
    }

    public int getStableSent() {
        return p.getStableSent();
    }

    public int getStableReceived() {
        return p.getStableReceived();
    }

    public int getStabilitySent() {
        return p.getStabilitySent();
    }

    public int getStabilityReceived() {
        return p.getStabilityReceived();
    }

    public void runMessageGarbageCollection() {
        p.runMessageGarbageCollection();
    }

}
