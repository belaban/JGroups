package org.jgroups.stack;

import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;
import org.jgroups.util.ShutdownRejectedExecutionHandler;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * If either {@link org.jgroups.protocols.UNICAST3} or {@link org.jgroups.protocols.pbcast.NAKACK2} are missing, then
 * this policy checks that the thread pool has a rejection policy of
 * {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}.<br/>
 * Issues a warning and changes the rejection policy to {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}
 * if not.
 * @author Bela Ban
 * @since  5.2.14
 */
public class CheckCallerRunsPolicy implements Policy {

    protected static final Class<? extends Protocol> UNICAST3=org.jgroups.protocols.UNICAST3.class,
      NAKACK2=org.jgroups.protocols.pbcast.NAKACK2.class;

    @Override
    public void check(Protocol prot) throws Exception {
        if(!(prot instanceof TP))
            throw new IllegalStateException(String.format("%s needs to be run in the scope of a transport",
                                                          CheckCallerRunsPolicy.class.getSimpleName()));
        TP tp=(TP)prot;
        ProtocolStack stack=tp.getProtocolStack();
        Log log=tp.getLog();
        if(stack.findProtocol(UNICAST3) == null || stack.findProtocol(NAKACK2) == null) {
            RejectedExecutionHandler handler=tp.getThreadPool().getRejectedExecutionHandler();
            if(handler != null && !isCallerRunsHandler(handler)) {
                log.warn("the absence of %s or %s requires CallerRunsPolicy in the thread pool; replacing %s",
                         UNICAST3.getSimpleName(), NAKACK2.getSimpleName(),
                         handler.getClass().getSimpleName());
                tp.getThreadPool().setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            }
        }
    }

    protected static boolean isCallerRunsHandler(RejectedExecutionHandler h) {
        return h instanceof ThreadPoolExecutor.CallerRunsPolicy ||
          (h instanceof ShutdownRejectedExecutionHandler
            && ((ShutdownRejectedExecutionHandler)h).handler() instanceof ThreadPoolExecutor.CallerRunsPolicy);
    }
}