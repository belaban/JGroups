package org.jgroups.blocks.executor;

import org.jgroups.JChannel;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.Executing;

/**
 * This class is to be used to pick up execution requests and actually run
 * them.  A single instance can  be used across any number of threads.
 * 
 * @author wburns
 */
public class ExecutionRunner implements Runnable {
    protected JChannel ch;
    protected Executing _execProt;
    
    public ExecutionRunner(JChannel channel) {
        setChannel(channel);
    }
    
    public void setChannel(JChannel ch) {
        this.ch=ch;
        _execProt=(Executing)ch.getProtocolStack().findProtocol(Executing.class);
        if(_execProt == null)
            throw new IllegalStateException("Channel configuration must include a executing protocol " +
                                              "(subclass of " + Executing.class.getName() + ")");
    }

    // @see java.lang.Runnable#run()
    @Override
    public void run() {
        Runnable runnable = null;
        // This task exits by being interrupted when the task isn't running
        for (;;) {
            runnable = (Runnable)ch.downcall(new ExecutorEvent(
                ExecutorEvent.CONSUMER_READY, null));
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                break;
            }
            Throwable throwable = null;
            try {
                runnable.run();
            }
            // This can only happen if user is directly doing an execute(Runnable)
            catch (Throwable t) {
                _logger.error("Unexpected Runtime Error encountered in Runnable request", t);
                throwable = t;
            }
            ch.down(new ExecutorEvent(ExecutorEvent.TASK_COMPLETE, 
                throwable != null ? new Object[]{runnable, throwable} : runnable));
            
            // If the interrupt status is still set then we treat that as
            // a shutdown.
            // TODO: there is still a hole that if a runnable is canceled interrupted at the same time this task is interrupted that we will lose the second interrupt.
            // TODO: instead maybe we should spawn a thread to handle the requests
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    protected static final Log _logger = LogFactory.getLog(ExecutionRunner.class);
}
