package org.jgroups.jmx;

import org.jgroups.Channel;

/**
 * @author Bela Ban
 * @version $Id: JChannelFactoryMBean.java,v 1.1 2006/04/18 15:26:50 belaban Exp $
 */
public interface JChannelFactoryMBean {
    void setMultiplexerConfig(String properties) throws Exception;
    Channel createMultiplexerChannel(String stack_name, String id) throws Exception;
    Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception;
    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();
    String dumpConfiguration();
    String dumpChannels();
}
