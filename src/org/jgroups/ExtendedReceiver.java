package org.jgroups;

/**
 * Extends Receiver, plus the partial state transfer methods.
 * This interface will disappear (be merged with Receiver) in 3.0.
 * @author Bela Ban
 */
public interface ExtendedReceiver extends Receiver, ExtendedMessageListener, ExtendedMembershipListener {
}
