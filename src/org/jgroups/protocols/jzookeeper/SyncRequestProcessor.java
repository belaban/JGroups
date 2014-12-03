package org.jgroups.protocols.jzookeeper;

import org.apache.log4j.Logger;
import org.jgroups.Message;
import org.slf4j.LoggerFactory;

public class SyncRequestProcessor {
	
	
	private static final Logger LOG = (org.apache.log4j.Logger) LoggerFactory.getLogger(CommitProcessor.class);

	
	   
	synchronized public void processMessage(Message proposalMessage) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing messageProposal (Sync to disk) " + proposalMessage);
        }
        
        //Do Sync message proposal to disk
    }
	
	
	

}
