package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: SEQUENCERMBean.java,v 1.1 2006/01/03 14:43:43 belaban Exp $
 */
public interface SEQUENCERMBean extends ProtocolMBean {
    boolean isCoord();
    String getCoordinator();
    String getLocalAddress();
    long getForwarded();
    long getBroadcast();
    long getReceivedForwards();
    long getReceivedBroadcasts();

}
