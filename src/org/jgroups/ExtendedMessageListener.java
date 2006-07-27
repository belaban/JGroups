package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * 
 * <code>ExtendedMessageListener</code> has additional callbacks for:
 * <ul>
 * <li>partial state transfer - http://jira.jboss.com/jira/browse/JGRP-118 
 * <li>streaming state transfer - http://jira.jboss.com/jira/browse/JGRP-89  
 * </ul>
 * <p>
 * Application channels interested in using streaming state transfer, beside 
 * implementing this interface, have to be configured with 
 * <code>STREAMING_STATE_TRANSFER</code> protocol rather than the default 
 * <code>STATE_TRANSFER</code> protocol. 
 * 
 * <p>
 * Note:
 * <p>
 * This interface will be merged with MessageListener in 3.0 (API changes)
 * 
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @see org.jgroups.JChannel#getState(Address, long)
 * @see org.jgroups.JChannel#getState(Address, String, long)
 * @since 2.3
 *  
 * @version $Id: ExtendedMessageListener.java,v 1.3 2006/07/27 18:44:01 vlada Exp $
 */
public interface ExtendedMessageListener extends MessageListener {
    
	/**
	 * Allows an application to provide a partial state as a byte array
	 * 
	 * @param state_id id of the partial state requested 
	 * @return partial state for the given state_id
	 */
	public byte[] getState(String state_id);
    
    /**
     * Allows an application to read a partial state indicated by state_id from 
     * a given state byte array parameter.
     * 
     * @param state_id id of the partial state requested
     * @param state partial state for the given state_id
     */
    public void setState(String state_id, byte[] state);
    
    /**
	 * Allows an application to write a state through a provided OutputStream. 
	 * An application is obligated to always close the given OutputStream reference. 
	 * 
	 * @param ostream the OutputStream
	 * @see OutputStream#close()
	 */
	public void getState(OutputStream ostream);
	
	/**
	 * Allows an application to write a partial state through a provided OutputStream. 
	 * An application is obligated to always close the given OutputStream reference. 
	 * 
	 * @param state_id id of the partial state requested 
	 * @param ostream the OutputStream
	 * 
	 * @see OutputStream#close()
	 */
	public void getState(String state_id,OutputStream ostream);
	
	
	/**
	 * Allows an application to read a state through a provided InputStream. 
	 * An application is obligated to always close the given InputStream reference. 
	 * 
	 * @param istream the InputStream
	 * @see InputStream#close()
	 */
	public void setState(InputStream istream);
	
	/**
	 * Allows an application to read a partial state through a provided InputStream. 
	 * An application is obligated to always close the given InputStream reference. 
	 * 
	 * @param state_id id of the partial state requested
	 * @param istream the InputStream
	 * 
	 * @see InputStream#close()
	 */
	public void setState(String state_id,InputStream istream);
}
