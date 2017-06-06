package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.View;

import java.util.Map;

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
     * Returns stats about the bundler itself.
     * @return Stats, may be null
     */
    default Map<String,Object> getStats() {return null;}

    default void resetStats() {}
}
