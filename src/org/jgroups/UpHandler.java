
package org.jgroups;

/**
 * Provides a hook to hijack over all events received by a certain channel which has installed this
 * UpHandler.
 * <p>
 * Client usually never need to implement this interface and it is mostly used by JGroups building
 * blocks.
 * 
 * @since 2.0
 * @author Bela Ban
 */
public interface UpHandler {
   
   /**
    * Invoked for all channel events except connection management and state transfer.
    * 
    * @param evt
    */
    Object up(Event evt);
}
