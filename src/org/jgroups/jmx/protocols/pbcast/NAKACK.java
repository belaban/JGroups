package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: NAKACK.java,v 1.14.2.2 2009/09/11 12:09:18 belaban Exp $
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

    /**
     *
     * @return
     * @deprecated removed in 2.6
     */
    public long getMaxXmitSize() {
        return -1;
    }

    /**
     * @param max_xmit_size
     * @deprecated removed in 2.6
     */
    public void setMaxXmitSize(long max_xmit_size) {
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

    public int getPendingRetransmissionRequests() {
        return p.getPendingRetransmissionRequests();
    }

    public long getUndeliveredMessages() {
        return p.getUndeliveredMessages();
    }

    public int getXmitTableSize() {
        return p.getXmitTableSize();
    }

    public String printXmitTable() {
        return p.printMessages();
    }

    public String printMessages() {
        return p.printMessages();
    }

    public String printStabilityMessages() {
        return p.printStabilityMessages();
    }

    public String printMergeHistory() {
        return p.printMergeHistory();
    }

    public String printRetransmissionAvgs() {
        return p.printRetransmissionAvgs();
    }

    public String printRetransmissionTimes() {
        return p.printRetransmissionTimes();
    }

    public String printSmoothedRetransmissionAvgs() {
        return p.printSmoothedRetransmissionAvgs();
    }

    public String printLossRates() {
        return p.printLossRates();
    }

    public double getTotalAvgXmitTime() {
        return p.getTotalAverageRetransmissionTime();
    }

    public double getTotalAvgSmoothedXmitTime() {
        return p.getTotalAverageSmoothedRetransmissionTime();
    }

    public int getAverageLossRate() {
        return (int)(p.getAverageLossRate() * 100);
    }

    public int getAverageSmoothedLossRate() {
        return (int)(p.getAverageSmoothedLossRate() * 100);
    }
}
