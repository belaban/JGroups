package org.jgroups.protocols.jzookeeper;

import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.jgroups.Message;
import org.slf4j.LoggerFactory;
public class CommitProcessor {
	
	 private static final Logger LOG = (org.apache.log4j.Logger) LoggerFactory.getLogger(CommitProcessor.class);

	    /**
	     * Message that we are holding until the commit comes in.
	     */
	    LinkedList<Message> queuedMessage = new LinkedList<Message>();

	    /**
	     * Message that have been committed.
	     */
	    LinkedList<Message> committedMessage = new LinkedList<Message>();
	    
	    FinalProcessor nextProcessor = null;
	    
	    public CommitProcessor() {
	        
	    }
	    
	    public CommitProcessor(FinalProcessor nextProcessor) {
	        this.nextProcessor = nextProcessor;
	    }

    synchronized public void commit(Message commitMessage) {
            if (commitMessage == null) {
                LOG.warn("Committed a null!",
                         new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + commitMessage);
            }
            committedMessage.add(commitMessage);
        
    }

     public void processMessage(Message proposalMessage) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + proposalMessage);
        }
        
            queuedMessage.add(proposalMessage);
        
    }

}
