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
        message counters etc */
    void init(TP transport);

    /** To reset stats */
    default void reset() {}

    /** Called before the transport is stopped */
    default void destroy() {}

    /**
     * Process a message that was not received from the transport but from above (e.g. the channel or a protocol), and
     * needs to be looped back up because (1) the destination address is null (every multicast message is looped back)
     * or (2) the destination address is the sender's address (unicast message to self).<p>
     * A message that is looped back can bypass cluster name matching.
     * @param msg the message to be looped back up the stack.
     * @param oob true if the message is an OOB message
     * @return
     */
    boolean loopback(Message msg, boolean oob);

    boolean loopback(MessageBatch batch, boolean oob);

    /**
     * Process a message received from the transport
     * @param msg the message
     * @param oob true if the message is an OOB message
     * @return
     */
    boolean process(Message msg, boolean oob);

    /**
     * Process a batch received from the transport
     * @param batch the batch
     * @param oob true if the batch contains only OOB messages
     * @return
     */
    boolean process(MessageBatch batch, boolean oob);
}
