
package org.jgroups;

/**
 * MessageListener allows a listener to be notified when a {@link Message} or a state transfer
 * events arrives to a node.
 * <p>
 * MessageListener is often used by JGroups building blocks installed on top of a channel i.e
 * RpcDispatcher and MessageDispatcher.
 * 
 * 
 * @see org.jgroups.blocks.RpcDispatcher
 * @see org.jgroups.blocks.MessageDispatcher
 * 
 * @since 2.0
 * @author Bela Ban
 * @author Vladimir Blagojevic
 */
public interface MessageListener extends StateListener {

   /**
    * Called when a message is received.
    * 
    * @param msg
    */
    void          receive(Message msg);
}
