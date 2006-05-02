package org.jgroups.jmx;

import org.jgroups.Channel;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactoryMBean.java,v 1.3 2006/05/02 11:06:00 belaban Exp $
 */
public interface JChannelFactoryMBean {
    String getMultiplexerConfig();
    void setMultiplexerConfig(String properties) throws Exception;

    String getDomain();
    void setDomain(String name);

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
