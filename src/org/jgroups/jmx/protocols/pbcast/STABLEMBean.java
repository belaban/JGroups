package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: STABLEMBean.java,v 1.2 2005/06/13 13:49:16 belaban Exp $
 */
public interface STABLEMBean extends ProtocolMBean {
    long getDesiredAverageGossip();
    void setDesiredAverageGossip(long gossip_interval);
    long getMaxBytes();
    void setMaxBytes(long max_bytes);
    int getNumberOfGossipMessages();
    void runMessageGarbageCollection();
}
