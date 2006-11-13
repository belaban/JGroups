package org.jgroups.blocks;

import org.jgroups.ChannelException;

/**
 * This exception is thrown when voting listener cannot vote on the
 * specified decree.
 *  
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class VoteException extends ChannelException {

    private static final long serialVersionUID = -878345689312038489L;

	public VoteException(String msg) { super(msg); }
}