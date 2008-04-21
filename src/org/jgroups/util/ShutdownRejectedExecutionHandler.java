package org.jgroups.util;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.LogFactory;

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
 * @version $Id: ShutdownRejectedExecutionHandler.java,v 1.101 2008/04/08
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

        if(executor.isShutdown()) {
            LogFactory.getLog(this.getClass()).warn("ThreadPoolExecutor " + executor
                                                    + " is shutdown and rejected submitted task "
                                                    + r);
        }
        else {
            handler.rejectedExecution(r, executor);
        }
    }
}
