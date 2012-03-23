package org.jgroups.protocols.pmcast.manager;

import org.jgroups.Message;

import java.util.List;

/**
 * The interface that a deliver manager must implement. This method is invoked by the deliver thread
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public interface DeliverManager {

    /**
     * returns an ordered list with the messages to be deliver.
     * This method blocks if no messages are ready to be deliver
     *
     * @return a list of messages to deliver
     * @throws InterruptedException if it is interrupted
     */
    List<Message> getNextMessagesToDeliver() throws InterruptedException;
}
