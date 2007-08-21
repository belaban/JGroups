package org.jgroups;

/**
 * Class which implements {@link org.jgroups.ChannelListener}
 * @author Bela Ban
 * @version $Id: ChannelListenerAdapter.java,v 1.1 2007/08/21 09:23:08 belaban Exp $
 */
public class ChannelListenerAdapter implements ChannelListener {
    public void channelConnected(Channel channel) {
    }

    public void channelDisconnected(Channel channel) {
    }

    public void channelClosed(Channel channel) {
    }

    public void channelShunned() {
    }

    public void channelReconnected(Address addr) {
    }
}
