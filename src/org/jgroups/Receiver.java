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
            catch(Throwable ignored) {
            }
        }
    }


    /**
     * Called when a change in membership has occurred. No long running actions, sending of messages
     * or anything that could block should be done in this callback. If some long running action
     * needs to be performed, it should be done in a separate thread.
     * <p>
     * Note that on reception of the first view (a new member just joined), the channel will not yet
     * be in the connected state. This only happens when {@link JChannel#connect(String)} returns.
     */
    default void viewAccepted(View new_view) {}


    /**
     * Allows an application to write the state to an OutputStream. After the state has
     * been written, the OutputStream doesn't need to be closed as stream closing is automatically
     * done when a calling thread returns from this callback.
     *
     * @param out The OutputStream
     * @throws Exception If the streaming fails, any exceptions should be thrown so that the state requester
     *                   can re-throw them and let the caller know what happened
     */
    default void getState(OutputStream out) throws Exception {
        throw new UnsupportedOperationException("getState() needs to be overridden by applications");
    }

    /**
     * Allows an application to read the state from an InputStream. After the state has been
     * read, the InputStream doesn't need to be closed as stream closing is automatically done when a
     * calling thread returns from this callback.
     *
     * @param in The InputStream
     * @throws Exception If the streaming fails, any exceptions should be thrown so that the state requester
     *                   can catch them and thus know what happened
     */
    default void setState(InputStream in) throws Exception {
        throw new UnsupportedOperationException("setState() needs to be overridden by applications");
    }
}
