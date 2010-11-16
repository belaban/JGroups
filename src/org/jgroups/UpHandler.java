
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
