package org.jgroups;

/**
 * Defines the callbacks that are invoked when messages, views etc are received on a channel
 * 
 * @see JChannel#setReceiver(Receiver)
 * @since 2.0
 * @author Bela Ban
 */
public interface Receiver extends MessageListener, MembershipListener {
}
