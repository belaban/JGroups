package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FRAG2MBean.java,v 1.1 2005/06/13 14:53:48 belaban Exp $
 */
public interface FRAG2MBean extends ProtocolMBean {
    int getFragSize();
    void setFragSize(int s);
    int getOverhead();
    void setOverhead(int o);
    long getNumberOfSentMessages();
    long getNumberOfSentFragments();
    long getNumberOfReceivedMessages();
    long getNumberOfReceivedFragments();
}
