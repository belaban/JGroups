package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;

import java.util.Properties;
import java.util.Vector;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: ProtocolMBean.java,v 1.6 2005/07/26 11:15:19 belaban Exp $
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
