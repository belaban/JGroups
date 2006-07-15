package org.jgroups;

/**
 * Defines the callbacks that are invoked when messages, views and streaming 
 * state requests are received on a channel
 * 
 * @author Vladimir Blagojevic
 * @since 2.4
 */
public interface StreamingReceiver extends StreamingMessageListener, MembershipListener {
}
