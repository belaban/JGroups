package org.jgroups;

/**
 * Extends Receiver, plus the partial state transfer methods.
 * This interface will disappear (be merged with Receiver) in 3.0.
 * @author Bela Ban
 * @version $Id: ExtendedReceiver.java,v 1.2 2006/09/27 12:39:14 belaban Exp $
 */
public interface ExtendedReceiver extends Receiver, ExtendedMessageListener, ExtendedMembershipListener {
}
