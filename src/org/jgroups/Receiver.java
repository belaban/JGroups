package org.jgroups;

import org.jgroups.util.MessageBatch;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Defines the callbacks that are invoked when messages, views etc are received
 * 
 * @see JChannel#setReceiver(Receiver)
 * @since 2.0
 * @author Bela Ban
 */
public interface Receiver {

    /**
     * Called when a message is received.
     * @param msg The message
     */
    default void receive(Message msg) {

    }

    /**
     * Called when a batch of messages is received
     * @param batch The message batch
     */
    default void receive(MessageBatch batch) {
        for(Message msg: batch) {
            try {
                receive(msg);
            }
            catch(Throwable t) {
            }
        }
    }


    /**
     * Called when a change in membership has occurred. No long running actions, sending of messages
     * or anything that could block should be done in this callback. If some long running action
     * needs to be performed, it should be done in a separate thread.
     * <br/>
     * Note that on reception of the first view (a new member just joined), the channel will not yet
     * be in the connected state. This only happens when {@link JChannel#connect(String)} returns.
     */
    default void viewAccepted(View new_view) {}


    /**
     * Called (usually by the {@link org.jgroups.protocols.pbcast.FLUSH} protocol), as an indication that the member
     * should stop sending messages. Any messages sent after returning from this callback might get blocked by the FLUSH
     * protocol. When the FLUSH protocol is done, and messages can be sent again, the FLUSH protocol
     * will simply unblock all pending messages. If a callback for unblocking is desired, implement
     * {@link org.jgroups.Receiver#unblock()}.
     */
    default void block() {}

    /**
     * Called <em>after</em> the FLUSH protocol has unblocked previously blocked senders, and
     * messages can be sent again.
     * <br/>
     * Note that during new view installation we provide guarantee that unblock invocation strictly
     * follows view installation at some node A belonging to that view. However, some other message
     * M may squeeze in between view and unblock callbacks.<br/>
     * For more details see https://jira.jboss.org/jira/browse/JGRP-986
     */
    default void unblock() {}

    /**
     * Allows an application to write the state to an OutputStream. After the state has
     * been written, the OutputStream doesn't need to be closed as stream closing is automatically
     * done when a calling thread returns from this callback.
     *
     * @param output The OutputStream
     * @throws Exception If the streaming fails, any exceptions should be thrown so that the state requester
     *                   can re-throw them and let the caller know what happened
     */
    default void getState(OutputStream output) throws Exception {
        throw new UnsupportedOperationException("getState() needs to be overridden by applications");
    }

    /**
     * Allows an application to read the state from an InputStream. After the state has been
     * read, the InputStream doesn't need to be closed as stream closing is automatically done when a
     * calling thread returns from this callback.
     *
     * @param input The InputStream
     * @throws Exception If the streaming fails, any exceptions should be thrown so that the state requester
     *                   can catch them and thus know what happened
     */
    default void setState(InputStream input) throws Exception {
        throw new UnsupportedOperationException("setState() needs to be overridden by applications");
    }
}
