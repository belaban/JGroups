package org.jgroups.util;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * ShutdownRejectedExecutionHandler is a decorator RejectedExecutionHandler used
 * in all JGroups ThreadPoolExecutor(s). Default RejectedExecutionHandler raises
 * RuntimeException when a task is submitted to ThreadPoolExecutor that has been
 * shutdown. ShutdownRejectedExecutionHandler instead logs only a warning
 * message.
 * <p>
 * A {@link ThreadPoolExecutor.DiscardPolicy} drops a rejected task <em>silently</em>, ie. without raising a
 * RejectedExecutionException. As callers such as {@link ThreadPool#execute(Runnable)} would otherwise treat a
 * discarded task as accepted, a RejectedExecutionException is raised on the policy's behalf.
 * 
 * @author Vladimir Blagojevic
 * @see ThreadPoolExecutor
 * @see RejectedExecutionHandler
 */
public class ShutdownRejectedExecutionHandler implements RejectedExecutionHandler {

    RejectedExecutionHandler handler;
    protected final boolean  discards; // true if the decorated handler drops tasks without raising an exception

    public ShutdownRejectedExecutionHandler(RejectedExecutionHandler handler) {
        super();
        this.handler=Objects.requireNonNull(handler);
        this.discards=handler instanceof ThreadPoolExecutor.DiscardPolicy;
    }

    public RejectedExecutionHandler handler() {return handler;}

    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if(!executor.isShutdown()) {
            handler.rejectedExecution(r, executor);
            if(discards) // the task was dropped silently: tell the caller that it will never be run
                throw new RejectedExecutionException(String.format("task %s was discarded", r));
        }
    }
}
