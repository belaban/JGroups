
package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Allows a listener to be notified when message or state transfer events arrive.
 */
public interface MessageListener {
	/**
	 * Called when a message is received. 
	 * @param msg
	 */
    void          receive(Message msg);


    /**
     * Allows an application to write a state through a provided OutputStream. When done, the OutputStream doesn't need
     * to be closed; this is done automatically when returning from the callback
     *
     * @param output the OutputStream
     * @throws Exception if the streaming fails, any exceptions should be thrown so that the state requester can
     * re-throw them and let the caller know what happened
     * @see java.io.OutputStream#close()
     */
    public void getState(OutputStream output) throws Exception;


    /**
     * Allows an application to read a state through a provided InputStream. When done, the InputStream doesn't need
     * to be closed; this is done automatically when returning from the callback
     *
     * @param input the InputStream
     * @throws Exception if the streaming fails, any exceptions should be thrown so that the state requester can
     * catch them and thus know what happened
     * @see java.io.InputStream#close()
     */
    public void setState(InputStream input) throws Exception;
}
