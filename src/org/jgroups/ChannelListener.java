// $Id: ChannelListener.java,v 1.2 2009/06/17 16:20:01 belaban Exp $

package org.jgroups;


/**
 * Allows a listener to be notified when important channel events occur. For example, when
 * a channel is closed, a PullPushAdapter can be notified, and stop accordingly.
 */
public interface ChannelListener {
    void channelConnected(Channel channel);
    void channelDisconnected(Channel channel);
    void channelClosed(Channel channel);
    @Deprecated void channelShunned();
    @Deprecated void channelReconnected(Address addr);
}
