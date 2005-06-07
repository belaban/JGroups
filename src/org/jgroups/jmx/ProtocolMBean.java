package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;

import java.util.Properties;
import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: ProtocolMBean.java,v 1.4 2005/06/07 12:29:51 belaban Exp $
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
    String dumpUpQueue();
    String dumpDownQueue();
    void setObserver(ProtocolObserver observer);

    String requiredUpServices();
    String requiredDownServices();
    String providedUpServices();
    String providedDownServices();


    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();

}
