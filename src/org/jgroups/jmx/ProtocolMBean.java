package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;

import java.util.Map;
import java.util.Properties;

/**
 * @author Bela Ban
 * @version $Id: ProtocolMBean.java,v 1.7.10.1 2007/04/27 08:03:50 belaban Exp $
 */
public interface ProtocolMBean {
    String getName();
    String getPropertiesAsString();
    void setProperties(Properties p);
    boolean getStatsEnabled();
    void setStatsEnabled(boolean flag);
    void resetStats();
    String printStats();
    Map dumpStats();
    boolean getUpThread();
    boolean getDownThread();
    void setObserver(ProtocolObserver observer);
    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();

}
