package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: NAKACK.java,v 1.4 2005/06/13 07:09:42 belaban Exp $
 */
public class NAKACK extends Protocol implements NAKACKMBean {
    org.jgroups.protocols.pbcast.NAKACK p;

    public NAKACK() {
    }

    public NAKACK(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.pbcast.NAKACK)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.pbcast.NAKACK)p;
    }

    public int getGcLag() {
        return p.getGcLag();
    }

    public void setGcLag(int gc_lag) {
        p.setGcLag(gc_lag);
    }

    public boolean isUseMcastXmit() {
        return p.isUseMcastXmit();
    }

    public void setUseMcastXmit(boolean use_mcast_xmit) {
        p.setUseMcastXmit(use_mcast_xmit);
    }

    public boolean isXmitFromRandomMember() {
        return p.isXmitFromRandomMember();
    }

    public void setXmitFromRandomMember(boolean xmit_from_random_member) {
        p.setXmitFromRandomMember(xmit_from_random_member);
    }

    public boolean isDiscardDeliveredMsgs() {
        return p.isDiscardDeliveredMsgs();
    }

    public void setDiscardDeliveredMsgs(boolean discard_delivered_msgs) {
        p.setDiscardDeliveredMsgs(discard_delivered_msgs);
    }

    public int getMaxXmitBufSize() {
        return p.getMaxXmitBufSize();
    }

    public void setMaxXmitBufSize(int max_xmit_buf_size) {
        p.setMaxXmitBufSize(max_xmit_buf_size);
    }

    public long getMaxXmitSize() {
        return p.getMaxXmitSize();
    }

    public void setMaxXmitSize(long max_xmit_size) {
        p.setMaxXmitSize(max_xmit_size);
    }

    public long getXmitRequestsReceived() {
        return p.getXmitRequestsReceived();
    }

    public long getXmitRequestsSent() {
        return p.getXmitRequestsSent();
    }

    public long getXmitResponsesReceived() {
        return p.getXmitResponsesReceived();
    }

    public long getXmitResponsesSent() {
        return p.getXmitResponsesSent();
    }

    public long getMissingMessagesReceived() {
        return p.getMissingMessagesReceived();
    }

    public String printSentMessages() {
        return p.printSentMsgs();
    }

    public String printMessages() {
        return p.printMessages();
    }

}
