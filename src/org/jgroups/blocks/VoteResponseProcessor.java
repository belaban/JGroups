package org.jgroups.blocks;

import org.jgroups.ChannelException;
import org.jgroups.util.RspList;


/**
 * VoteResultProcessor
 * Applications that use the VotingAdapter and/or TwoPhaseVotingAdapter can pass an implementation of this down the vote
 * calls, to intercept processing of the VoteResults returned by other nodes.
 * See the source of @see org.jgroups.blocks.DistributedLockManager for an example implementation.  
 * 
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: VoteResponseProcessor.java,v 1.1 2005/06/08 15:56:54 publicnmi Exp $
 */
public interface VoteResponseProcessor {
    /**
     * Processes the responses returned by the other nodes
     * @param responses The responses
     * @param consensusType The consensusType of the vote
     * @param decree The vote decree
     * @return
     * @throws ChannelException
     */
    public boolean processResponses(RspList responses, int consensusType, Object decree) throws ChannelException;
}
