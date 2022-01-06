
package org.jgroups;

import org.jgroups.util.MessageBatch;

/**
 * Provides a hook to hijack all events received by a certain channel which has installed this UpHandler.<p>
 * Client usually never need to implement this interface and it is mostly used by JGroups building blocks.
 * @since 2.0
 * @author Bela Ban
 */
public interface UpHandler {

    UpHandler setLocalAddress(Address a);

    Object up(Event evt);

    Object up(Message msg);

    default void up(MessageBatch batch) {
        for(Message msg: batch) {
            try {
                up(msg);
            }
            catch(Throwable t) {
            }
        }
    }
}
