package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: FRAG2.java,v 1.1 2005/06/13 14:53:48 belaban Exp $
 */
public class FRAG2 extends Protocol implements FRAG2MBean {
    org.jgroups.protocols.FRAG2 p;

    public FRAG2() {
    }

    public FRAG2(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.FRAG2)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.FRAG2)p;
    }

    public int getFragSize() {
        return p.getFragSize();
    }

    public void setFragSize(int s) {
        p.setFragSize(s);
    }

    public int getOverhead() {
        return p.getOverhead();
    }

    public void setOverhead(int o) {
        p.setOverhead(o);
    }

    public long getNumberOfSentMessages() {
        return p.getNumberOfSentMessages();
    }

    public long getNumberOfSentFragments() {
        return p.getNumberOfSentFragments();
    }

    public long getNumberOfReceivedMessages() {
        return p.getNumberOfReceivedMessages();
    }

    public long getNumberOfReceivedFragments() {
        return p.getNumberOfReceivedFragments();
    }
}
