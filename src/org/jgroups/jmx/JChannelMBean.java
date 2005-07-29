package org.jgroups.jmx;

import org.jgroups.*;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: JChannelMBean.java,v 1.5 2005/07/29 08:59:36 belaban Exp $
 */
public interface JChannelMBean {
    void create() throws Exception;
    void start() throws Exception;
    void stop();
    void destroy();

    //void jbossInternalLifecycle(String method) throws Exception;
    org.jgroups.JChannel getChannel();

    String getProperties();
    void setProperties(String props);

    String getVersion();

    String getObjectName();
    void setObjectName(String name);

    /** To configure via XML file */
    void setClusterConfig(Element el);

    String getGroupName();
    void setGroupName(String group_name);

    String getClusterName();
    void setClusterName(String cluster_name);

    boolean getReceiveViewEvents();
    void setReceiveViewEvents(boolean flag);

    boolean getReceiveSuspectEvents();
    void setReceiveSuspectEvents(boolean flag);

    boolean getReceiveBlockEvents();
    void setReceiveBlockEvents(boolean flag);

    boolean getReceiveStateEvents();
    void setReceiveStateEvents(boolean flag);

    boolean getReceiveLocalMessages();
    void setReceiveLocalMessages(boolean flag);

    boolean getAutoReconnect();
    void setAutoReconnect(boolean flag);

    boolean getAutoGetState();
    void setAutoGetState(boolean flag);

    Map dumpStats();

    View getView();
    String getViewAsString();
    Address getLocalAddress();
    String getLocalAddressAsString();
    void setChannelListener(ChannelListener channel_listener);
    boolean getStatsEnabled();
    void setStatsEnabled(boolean flag);
    void resetStats();
    long getSentMessages();
    long getSentBytes();
    long getReceivedMessages();
    long getReceivedBytes();

    boolean isOpen();

    boolean isConnected();

    int getNumMessages();

    String dumpQueue();

    String printProtocolSpec(boolean include_properties);

    String toString(boolean print_details);

    void connect(String channel_name) throws ChannelException, ChannelClosedException;

    void disconnect();

    void close();

    void shutdown();

    void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException;

    void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException;

    void sendToAll(String msg) throws ChannelNotConnectedException, ChannelClosedException;

    /** @param evt
     * @deprecated */
    void down(Event evt);

    Object receive(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException;

    Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException;

    void blockOk();

    boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException;

    void returnState(byte[] state);
}
