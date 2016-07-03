package org.jgroups;

/**
 * Allows a listener to be notified when an important channel lifecycle event occurs.
 * <p>
 * 
 * Usually clients do not need to implement ChannelListener interface. However, this interface can
 * useful in scenarios when an application opens multiple channels and needs to tracks major
 * lifecycle events on those channels from a single location or in scenarios when channel is wrapped
 * by JGroups building block installed on top of a channel (RpcDispatcher etc) while a client needs
 * to be notified about major channel lifecycle events.
 * 
 * @see JChannel#addChannelListener(ChannelListener)
 * @see JChannel#removeChannelListener(ChannelListener)
 * @see JChannel#clearChannelListeners()
 * 
 * @author Bela Ban
 * @since 2.0
 * 
 */
public interface ChannelListener {

   /**
    * Channel has been connected notification callback
    * 
    * @param channel the channel that has been connected
    */
   void channelConnected(JChannel channel);

   /**
    * Channel has been disconnected notification callback
    * 
    * @param channel the disconnected channel
    */
   void channelDisconnected(JChannel channel);

   /**
    * Channel has been closed notification callback
    * 
    * @param channel the closed channel
    */
   void channelClosed(JChannel channel);
}
