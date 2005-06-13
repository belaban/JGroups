package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.Protocol;
import org.jgroups.protocols.pbcast.*;

/**
 * @author Bela Ban
 * @version $Id: STABLE.java,v 1.1 2005/06/13 07:09:43 belaban Exp $
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

    public void runMessageGarbageCollection() {
        p.runMessageGarbageCollection();
    }

}
