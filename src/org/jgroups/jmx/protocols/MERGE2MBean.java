package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: MERGE2MBean.java,v 1.1 2005/06/13 11:55:31 belaban Exp $
 */
public interface MERGE2MBean extends ProtocolMBean {
    long getMinInterval();
    void setMinInterval(long i);
    long getMaxInterval();
    void setMaxInterval(long l);
}
