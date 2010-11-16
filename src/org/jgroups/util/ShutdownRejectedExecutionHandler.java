package org.jgroups.util;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * ShutdownRejectedExecutionHandler is a decorator RejectedExecutionHandler used
 * in all JGroups ThreadPoolExecutor(s). Default RejectedExecutionHandler raises
 * RuntimeException when a task is submitted to ThreadPoolExecutor that has been
 * shutdown. ShutdownRejectedExecutionHandler instead logs only a warning
 * message.
 * 
 * @author Vladimir Blagojevic
 * @see ThreadPoolExecutor
 * @see RejectedExecutionHandler
 *          14:49:05 belaban Exp $
 */
public class ShutdownRejectedExecutionHandler implements RejectedExecutionHandler {

    RejectedExecutionHandler handler;

    public ShutdownRejectedExecutionHandler(RejectedExecutionHandler handler) {
        super();
        if(handler == null)
            throw new NullPointerException("RejectedExecutionHandler cannot be null");
        this.handler=handler;
    }

    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

        if(!executor.isShutdown()) {
            handler.rejectedExecution(r, executor);
        }
    }
}
