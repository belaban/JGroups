package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Detects sends during processing of incoming messages and sets flag
 * {@link org.jgroups.Message.TransientFlag#DONT_BLOCK} to prevent blocking the incoming thread on a send.<p>
 * JIRA: https://issues.redhat.com/browse/JGRP-2772
 * @author Bela Ban
 * @since  5.3.5
 */
@MBean(description="Prevents blocking sends during the processing of an incoming message")
public class NON_BLOCKING_SENDS extends Protocol {

    @Property(description="Lists messages sent during a receive that are not marked as DONT_BLOCK")
    protected boolean         verbose;

    @Property(description="When a message send during reception is detected, set DONT_BLOCK if fix==true")
    protected boolean         fix=true;

    @ManagedAttribute(description="The threads currently processing received messages")
    protected final Set<Long> threads=new ConcurrentSkipListSet<>();

    @Override
    public Object down(Message msg) {
        long tid=threadId();

        if(threads.contains(tid) && !msg.isFlagSet(Message.TransientFlag.DONT_BLOCK)) {
            if(log.isTraceEnabled())
                log.trace("%s: setting DONT_BLOCK in message %s: hdrs: %s", local_addr, msg, msg.printHeaders());
            if(fix)
                msg.setFlag(Message.TransientFlag.DONT_BLOCK);
        }


        return down_prot.down(msg);
    }

    @Override
    public Object up(Message msg) {
        long tid=threadId();
        threads.add(tid);
        try {
            return up_prot.up(msg);
        }
        finally {
            threads.remove(tid);
        }
    }

    @Override
    public void up(MessageBatch batch) {
        long tid=threadId();
        threads.add(tid);
        try {
            up_prot.up(batch);
        }
        finally {
            threads.remove(tid);
        }
    }

    protected static long threadId() {
        //noinspection deprecation
        return Thread.currentThread().getId();
    }
}
