package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: FRAG.java,v 1.1 2005/06/13 14:29:28 belaban Exp $
 */
public class FRAG extends Protocol implements FRAGMBean {
    org.jgroups.protocols.FRAG p;

    public FRAG() {
    }

    public FRAG(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.FRAG)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.FRAG)p;
    }

    public int getFragSize() {
        return p.getFragSize();
    }

    public void setFragSize(int s) {
        p.setFragSize(s);
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
