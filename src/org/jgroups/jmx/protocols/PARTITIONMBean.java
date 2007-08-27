package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

import java.net.InetAddress;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: PARTITIONMBean.java,v 1.1 2007/08/27 10:28:36 belaban Exp $
 */
public interface PARTITIONMBean extends PINGMBean {
    boolean isPartitionOn();
    void startPartition();
    void stopPartition();
}