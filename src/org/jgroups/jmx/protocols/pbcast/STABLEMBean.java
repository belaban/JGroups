package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: STABLEMBean.java,v 1.3 2005/08/26 14:19:08 belaban Exp $
 */
public interface STABLEMBean extends ProtocolMBean {
    long getDesiredAverageGossip();
    void setDesiredAverageGossip(long gossip_interval);
    long getMaxBytes();
    void setMaxBytes(long max_bytes);
    int getGossipMessages();
    void runMessageGarbageCollection();
}
