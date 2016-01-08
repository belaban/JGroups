package org.jgroups.fork;

import org.jgroups.Message;

/**
 * Allows a user of fork to define the handling of a message for which no fork stack or fork channel exists.
 * @author Paul Ferraro
 */
public interface UnknownForkHandler {

    /**
     * Handle a message that refers to an unknown fork stack
     * @param message an incoming message
     * @param forkStackId the identifier of a fork stack
     * @return the result of the up handler
     */
    Object handleUnknownForkStack(Message message, String forkStackId);

    /**
     * Handle a message that refers to an unknown fork channel
     * @param message an incoming message
     * @param forkChannelId the identifier of a fork channel
     * @return the result of the up handler
     */
    Object handleUnknownForkChannel(Message message, String forkChannelId);
}
