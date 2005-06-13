package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FRAGMBean.java,v 1.1 2005/06/13 14:29:28 belaban Exp $
 */
public interface FRAGMBean extends ProtocolMBean {
    int getFragSize();
    void setFragSize(int s);
    long getNumberOfSentMessages();
    long getNumberOfSentFragments();
    long getNumberOfReceivedMessages();
    long getNumberOfReceivedFragments();
}
