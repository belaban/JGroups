package org.jgroups;
import java.io.InputStream;
/**
 * 
 * Represents an event returned by <code>channel.receive()</code>, as requested by
 * <code>channel.getState()</code> previously.
 * 
 * <p>
 * 
 * Allows applications using a channel in a pull mode to receive a state from 
 * another channel instance providing state. Channels have to be configured with
 * <code>STREAMING_STATE_TRANSFER</code> protocol rather than the default 
 * <code>STATE_TRANSFER</code> protocol in order to receive this event. 
 * 
 * <p>
 * 
 * The following code demonstrate how to pull events from a channel, processing 
 * <code>StreamingSetStateEvent</code> and retrieving hypothetical state in the 
 * form of LinkedList from event's <code>InputStream</code> reference.
 * 
 * <blockquote><pre>
 *  Object obj=channel.receive(0);
 *  if(obj instanceof StreamingSetStateEvent) {
 *   	StreamingSetStateEvent evt=(StreamingSetStateEvent)obj;
 *    	ObjectInputStream ois = null;   	
 *		try {			
 *			ois = new ObjectInputStream(evt.getArg());
 *			state = (LinkedList)ois.readObject();  
 *		} catch (Exception e) {} 
 *		finally
 *		{
 *			try {				
 *				ois.close();
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
 * @see org.jgroups.StreamingMessageListener#setState(InputStream)
 * @since 2.4
 * 
 */
public class StreamingSetStateEvent {

	InputStream is;
	String state_id;
	
	public StreamingSetStateEvent(InputStream is,String state_id) {
		super();
		this.is=is;
		this.state_id=state_id;
	}
	
	/**
	 * Returns InputStream used for reading of a state.
	 * 
	 * @return the InputStream
	 */
	public InputStream getArg()
	{
		return is;
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
