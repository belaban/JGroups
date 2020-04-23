package org.jgroups;

import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  5.0
 */
public interface MessageFactory {


    /**
     * Creates a message based on the given ID
     * @param id The ID
     * @param <T> The type of the message
     * @return A message
     */
    <T extends Message> T create(short id);

    /**
     * Registers a new creator of messages
     * @param type The type associated with the new payload. Needs to be the same in all nodes of the same cluster, and
     *             needs to be available (ie., not taken by JGroups or other applications).
     * @param generator The creator of the payload associated with the given type
     */
    <M extends MessageFactory> M register(short type, Supplier<? extends Message> generator);
}
