package org.jgroups.blocks.executor;

import java.util.concurrent.atomic.AtomicBoolean;

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
        final AtomicBoolean shutdown = new AtomicBoolean();
        // This thread is only spawned so that we can differentiate between
        // an interrupt of a task and an interrupt causing a shutdown of
        // runner itself.
        Thread executionThread = new Thread() {

            // @see java.lang.Thread#run()
            @Override
            public void run() {
                Runnable runnable = null;
                // This task exits by being interrupted when the task isn't running
                while (!shutdown.get()) {
                    runnable = (Runnable)ch.downcall(new ExecutorEvent(
                        ExecutorEvent.CONSUMER_READY, null));
                    if (Thread.interrupted()) {
                        if (runnable != null) {
                            // We assume that if an interrupt occurs here that
                            // it is trying to close down the task.  Since the
                            // window is so small.  Therefore if we get a
                            // task we need to reject it so it can be passed
                            // off to a different consumer
                            ch.down(new ExecutorEvent(ExecutorEvent.TASK_COMPLETE, 
                                new Object[]{runnable, new InterruptedException()}));
                        }
                        continue;
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
                }
            }
        };

        executionThread.setName(Thread.currentThread().getName() + "- Task Runner");
        executionThread.start();
        
        try {
            executionThread.join();
        }
        catch (InterruptedException e) {
            shutdown.set(true);
            executionThread.interrupt();
            if (_logger.isTraceEnabled()) {
                _logger.trace("Shutting down Execution Runner");
            }
        }
    }
    
    protected static final Log _logger = LogFactory.getLog(ExecutionRunner.class);
}
