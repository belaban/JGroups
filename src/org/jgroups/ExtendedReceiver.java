package org.jgroups;

/**
 * Extends Receiver, plus the partial state transfer methods.
 * This interface will disappear (be merged with Receiver) in 3.0.
 * @author Bela Ban
 * @version $Id: ExtendedReceiver.java,v 1.1 2006/03/16 09:56:28 belaban Exp $
 */
public interface ExtendedReceiver extends Receiver, ExtendedMessageListener {
}
