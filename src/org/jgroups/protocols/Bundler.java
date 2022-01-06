package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.View;

/**
 * Pluggable way to collect messages and send them as batches
 * @author Bela Ban
 * @since  4.0
 */
public interface Bundler {


    /**
     * Called after creation of the bundler
     * @param transport the transport, for further reference
     */
    default void init(@SuppressWarnings("UnusedParameters") TP transport) {}
    /** Called after {@link #init(TP)} */
    void start();
    void stop();
    void send(Message msg) throws Exception;
    @SuppressWarnings("UnusedParameters")
    default void viewChange(View view) {}

    /** The number of unsent messages in the bundler */
    int size();

    /**
     * If the bundler has a queue and it should be managed by a queuing discipline (like Random Early Detection), then
     * return the number of elements in the queue, else -1. In the latter case, the queue won't be managed.<br/>
     * This method needs to be fast as it might get called on every message to be sent.
     */
    int getQueueSize();

    /**
     * If the bundler implementation supports a capacity (e.g. {@link RingBufferBundler}, then return it, else return -1
     */
    default int getCapacity() {return -1;}

    /** Maximum number of bytes for messages to be queued until they are sent */
    int             getMaxSize();
    default Bundler setMaxSize(int s) {return this;}

    default void resetStats() {}
}
