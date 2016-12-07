package org.jgroups.stack;

import org.jgroups.Message;
import org.jgroups.protocols.TP;
import org.jgroups.util.MessageBatch;

/**
 * Policy which decides how to process a received message or message batch. Example: pass the message or batch to
 * the thread pool (default impl), or pass only one (unicast and mulicast) message per sender to the thread pool at a
 * time and queue others from the same sender.
 * @author Bela Ban
 * @since  4.0
 */
public interface MessageProcessingPolicy {
    /** Called after creation. Implementations may want to cache the transport reference to get access to thread pools,
     * message counters etc */
    void init(TP transport);

    /** Called before the transport is destroyed */
    default void destroy() {}

    void process(Message msg);

    void process(MessageBatch batch, boolean oob, boolean internal);
}
