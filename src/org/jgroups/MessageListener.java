
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
     * Answers the group state; e.g., when joining.
     * @return byte[] 
     */
    byte[]        getState();
    /**
     * Sets the group state; e.g., when joining.
     * @param state
     */
    void          setState(byte[] state);

    /**
     * Allows an application to write a state through a provided OutputStream.
     * An application is obligated to always close the given OutputStream reference.
     *
     * @param ostream the OutputStream
     * @see java.io.OutputStream#close()
     */
    public void getState(OutputStream ostream);


    /**
     * Allows an application to read a state through a provided InputStream.
     * An application is obligated to always close the given InputStream reference.
     *
     * @param istream the InputStream
     * @see java.io.InputStream#close()
     */
    public void setState(InputStream istream);
}
