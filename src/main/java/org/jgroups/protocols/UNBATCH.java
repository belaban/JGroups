package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/**
 * Intercepts {@link org.jgroups.stack.Protocol#up(MessageBatch)} and passes up each message of a message batch
 * as a single message. Mainly to be used in unit tests (https://issues.redhat.com/browse/JGRP-2702).
 * @author Bela Ban
 * @since  5.2.18
 */
@MBean(description="Passes each message from a MessageBatch up as a single message")
public class UNBATCH extends Protocol {
    @Property(description="If enabled, message batches are passed up as single messages, otherwise as batches")
    protected boolean enabled=true;

    public boolean enabled()         {return enabled;}
    public UNBATCH enable(boolean b) {enabled=b; return this;}

    @Override
    public void up(MessageBatch batch) {
        if(!enabled) {
            up_prot.up(batch);
            return;
        }
        for(Message msg: batch)
            up_prot.up(msg);
    }
}
