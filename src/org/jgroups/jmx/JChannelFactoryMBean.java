package org.jgroups.jmx;

import org.jgroups.Channel;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactoryMBean.java,v 1.2 2006/04/26 22:14:12 belaban Exp $
 */
public interface JChannelFactoryMBean {
    String getMultiplexerConfig();
    void setMultiplexerConfig(String properties) throws Exception;

    String getObjectName();
    void setObjectName(String name);

    boolean isExposeChannels();
    void setExposeChannels(boolean flag);

    boolean isExposeProtocols();
    void setExposeProtocols(boolean f);

    Channel createMultiplexerChannel(String stack_name, String id) throws Exception;
    Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception;
    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();
    String dumpConfiguration();
    String dumpChannels();
}
