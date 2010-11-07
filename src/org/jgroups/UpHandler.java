// $Id: UpHandler.java,v 1.4 2007/01/11 12:57:28 belaban Exp $

package org.jgroups;

/**
 * Provides a way of taking over a channel's tasks. 
 */
public interface UpHandler {
	/**
	 * Invoked for all channel events except connection management and state transfer.
	 * @param evt
	 */
    Object up(Event evt);
}
