package org.jgroups.blocks;

import org.jgroups.ChannelException;

/**
 * This exception is thrown when voting listener cannot vote on the
 * specified decree.
 *  
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class VoteException extends ChannelException {

    public VoteException(String msg) { super(msg); }
}