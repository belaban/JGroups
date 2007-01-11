// $Id: UpHandler.java,v 1.3 2007/01/11 11:38:41 belaban Exp $

package org.jgroups;

/**
 * Provides a way of taking over a channel's tasks. 
 */
public interface UpHandler {
	/**
	 * Invoked for all channel events except connection management and state transfer.
	 * @param evt
	 */
    void up(Event evt);

    Object upcall(Event evt);
}
