package org.jgroups.jmx;

import org.jgroups.Channel;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactoryMBean.java,v 1.3.10.1 2007/11/28 11:39:57 belaban Exp $
 */
public interface JChannelFactoryMBean {
    String getMultiplexerConfig();
    void setMultiplexerConfig(String properties) throws Exception;
    void setMultiplexerConfig(String properties, boolean replace) throws Exception;

    String getDomain();
    void setDomain(String name);

    boolean isExposeChannels();
    void setExposeChannels(boolean flag);

    boolean isExposeProtocols();
    void setExposeProtocols(boolean f);

    String getConfig(String stack_name) throws Exception;
    boolean removeConfig(String stack_name);
    void clearConfigurations();


    Channel createMultiplexerChannel(String stack_name, String id) throws Exception;
    Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception;
    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();
    String dumpConfiguration();
    String dumpChannels();
}
