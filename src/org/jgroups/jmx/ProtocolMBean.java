package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;

import java.util.Properties;
import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: ProtocolMBean.java,v 1.5 2005/06/13 15:50:38 belaban Exp $
 */
public interface ProtocolMBean {
    String getName();
    String getPropertiesAsString();
    void setProperties(Properties p);
    boolean getStatsEnabled();
    void setStatsEnabled(boolean flag);
    void resetStats();
    String printStats();
    boolean getUpThread();
    boolean getDownThread();
    void setObserver(ProtocolObserver observer);
    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();

}
