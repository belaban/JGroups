package org.jgroups.blocks.executor;

import org.jgroups.JChannel;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.Executing;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
        _execProt=ch.getProtocolStack().findProtocol(Executing.class);
        if(_execProt == null)
            throw new IllegalStateException("Channel configuration must include a executing protocol " +
                                              "(subclass of " + Executing.class.getName() + ")");
    }
    
    protected static class Holder<T> {
        protected T value;
        
        public Holder(T value) {
            this.value = value;
        }
    }

    // @see java.lang.Runnable#run()
    @Override
    public void run() {
        final Lock shutdownLock = new ReentrantLock();
        
        // The following 2 atomic boolean should only ever be updated while
        // protected by the above lock.  They don't have to be atomic boolean,
        // but it is a nice wrapper to share a reference between threads.
        // Reads can be okay in certain circumstances
        final AtomicBoolean canInterrupt = new AtomicBoolean(true);
        final AtomicBoolean shutdown = new AtomicBoolean();

        // This thread is only spawned so that we can differentiate between
        // an interrupt of a task and an interrupt causing a shutdown of
        // runner itself.
        Thread executionThread = new Thread() {

            // @see java.lang.Thread#run()
            @Override
            public void run() {
                
                Thread currentThread = Thread.currentThread();
                Runnable runnable = null;
                // This task exits by being interrupted when the task isn't running
                // or by updating shutdown to true when it can't be interrupted
                while (!shutdown.get()) {
                    _runnables.put(currentThread, new Holder<>(null));
                    runnable = (Runnable)ch.down(new ExecutorEvent(ExecutorEvent.CONSUMER_READY, null));
                    
                    // This means we were interrupted while waiting
                    if (runnable == null)
                        break;

                    // First retrieve the lock to make sure we can tell them
                    // to not interrupt us
                    shutdownLock.lock();
                    try {
                        // Clear interrupt state as we don't want to stop the
                        // task we just received.  If we got a shutdown signal
                        // we will only do it after we loop back around.
                        Thread.interrupted();
                        canInterrupt.set(false);
                    }
                    finally {
                        shutdownLock.unlock();
                    }
                    _runnables.put(currentThread, new Holder<>(runnable));
                    
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
                    
                    // We have to let the outer thread know we can now be interrupted
                    shutdownLock.lock();
                    try {
                        canInterrupt.set(true);
                    }
                    finally {
                        shutdownLock.unlock();
                    }
                }
                
                _runnables.remove(currentThread);
            }
        };

        executionThread.setName(Thread.currentThread().getName() + "- Task Runner");
        executionThread.start();
        
        try {
            executionThread.join();
        }
        catch (InterruptedException e) {
            shutdownLock.lock();
            try {
                if (canInterrupt.get()) {
                    executionThread.interrupt();
                }
                shutdown.set(true);
            }
            finally {
                shutdownLock.unlock();
            }
            
            if (_logger.isTraceEnabled()) {
                _logger.trace("Shutting down Execution Runner");
            }
        }
    }
    
    /**
     * Returns a copy of the runners being used with the runner and what threads.
     * If a thread is not currently running a task it will return with a null
     * value.  This map is a copy and can be modified if necessary without
     * causing issues.
     * @return map of all threads that are active with this runner.  If the
     *         thread is currently running a job the runnable value will be
     *         populated otherwise null would mean the thread is waiting
     */
    public Map<Thread, Runnable> getCurrentRunningTasks() {
        Map<Thread, Runnable> map = new HashMap<>();
        for (Entry<Thread, Holder<Runnable>> entry : _runnables.entrySet()) {
            map.put(entry.getKey(), entry.getValue().value);
        }
        return map;
    }
    
    private final Map<Thread, Holder<Runnable>> _runnables = 
            new ConcurrentHashMap<>();
    
    protected static final Log _logger = LogFactory.getLog(ExecutionRunner.class);
}
