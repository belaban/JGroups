package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 *  
 * Applications implementing <code>StreamingMessageListener</code> interface are 
 * notified when a request for a streaming state transfer arrives. Besides 
 * implementing this interface channels have to be configured with 
 * <code>STREAMING_STATE_TRANSFER</code> protocol rather than the default 
 * <code>STATE_TRANSFER</code> protocol in order to receive <code>getState</code> 
 * and <code>setState</code> callbacks. 
 * 
 * @author Vladimir Blagojevic
 * @see org.jgroups.blocks.PullPushAdapter
 * @see org.jgroups.demos.Chat
 * @since 2.4
 *  
 */
public interface StreamingMessageListener extends MessageListener {
	
	/**
	 * Allows an application to write a state through a provided OutputStream. 
	 * An application is obligated to always close the given OutputStream reference. 
	 * 
	 * @param ostream the OutputStream
	 * @see OutputStream#close()
	 */
	public void getState(OutputStream ostream);
	
	/**
	 * Allows an application to read a state through a provided InputStream. 
	 * An application is obligated to always close the given InputStream reference. 
	 * 
	 * @param istream the InputStream
	 * @see InputStream#close()
	 */
	public void setState(InputStream istream);

}
