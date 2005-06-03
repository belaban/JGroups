package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;

import java.util.Properties;
import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: ProtocolMBean.java,v 1.1 2005/06/03 08:49:18 belaban Exp $
 */
public interface ProtocolMBean {
    String getName();
    String getPropertiesAsString();
    void setProperties(Properties p);
    boolean getUpThread();
    boolean getDownThread();
    int getUpEvents();
    int getDownEvents();
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
