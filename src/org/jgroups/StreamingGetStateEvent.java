package org.jgroups;
   
import java.io.OutputStream;

/**
 * 
 * Represents an event returned by <code>channel.receive()</code>, as a result 
 * of another channel instance requesting a state from this channel. Other channel 
 * has to invoke <code>channel.getState()</code> indicating intent of state 
 * retrieval. 
 * 
 * <p>
 * 
 * Allows applications using a channel in a pull mode to receive 
 * <code>StreamingGetStateEvent</code> event and thus provide state to requsting 
 * channel instance. Channels have to be configured with 
 * <code>STREAMING_STATE_TRANSFER</code> protocol rather than the default 
 * <code>STATE_TRANSFER</code> protocol in order to receive this event 
 *  
 * <p>
 * 
 * The following code demonstrates how to pull events from a channel, processing 
 * <code>StreamingGetStateEvent</code> and sending hypothetical state through 
 * <code>OutputStream</code> reference.
 * 
 * <blockquote><pre>
 *  Object obj=channel.receive(0);
 *  if(obj instanceof StreamingGetStateEvent) {
 *   	StreamingGetStateEvent evt=(StreamingGetStateEvent)obj;
 *    	OutputStream oos = null;
 *		try {			
 *			oos = new ObjectOutputStream(evt.getArg());			
 *			oos.writeObject(state);   
 *	    	oos.flush();
 *		} catch (Exception e) {} 
 *		finally
 *		{
 *			try {				
 *				oos.close();
 *			} catch (IOException e) {
 *				System.err.println(e);
 *			}
 *		}                
 *   }
 * </pre></blockquote>
 * 
 * 
 * @author Vladimir Blagojevic
 * @see org.jgroups.JChannel#getState(Address, long)
 * @see org.jgroups.StreamingMessageListener#getState(OutputStream)
 * @since 2.4
 * 
 */

public class StreamingGetStateEvent {

	OutputStream os;
	String state_id;
	
	public StreamingGetStateEvent(OutputStream os,String state_id) {
		super();
		this.os=os;
		this.state_id=state_id;
	}
	/**
	 * Returns OutputStream used for writing of a state.
	 * 
	 * @return the OutputStream
	 */
	public OutputStream getArg()
	{
		return os;
	}
	
	/**
	 * Returns id of the partial state if partial state was requested. 
	 * If full state transfer was requested this method will return null.
	 * 
	 * @see JChannel#getState(Address, long)
	 * @see JChannel#getState(Address, String, long)
	 * @return partial state id 
	 */
	public String getStateId()
	{
		return state_id;
	}	

}
