package org.jgroups.stack;

import org.jgroups.protocols.RED;
import org.jgroups.protocols.TP;
import org.jgroups.util.ShutdownRejectedExecutionHandler;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * If either {@link org.jgroups.protocols.UNICAST3} or {@link org.jgroups.protocols.pbcast.NAKACK2} are missing, then
 * this policy checks that {@link org.jgroups.protocols.RED} is <em>not</em> in the protocol stack.
 * @author Bela Ban
 * @since 5.2.14
 */
public class CheckForAbsenceOfRED implements Policy {

    protected static final Class<? extends Protocol> UNICAST3=org.jgroups.protocols.UNICAST3.class,
      NAKACK2=org.jgroups.protocols.pbcast.NAKACK2.class, RED=RED.class;

    @Override
    public void check(Protocol prot) throws Exception {
        if(!(prot instanceof TP))
            throw new IllegalStateException(String.format("%s needs to be run in the scope of a transport",
                                                          CheckForAbsenceOfRED.class.getSimpleName()));
        ProtocolStack stack=prot.getProtocolStack();
        if(stack.findProtocol(UNICAST3) == null || stack.findProtocol(NAKACK2) == null) {
            if(stack.findProtocol(RED) != null) {
                String e=String.format("found %s: when either %s or %s are missing, this can lead " +
                                         "to message loss. Please remove %s", RED.getSimpleName(),
                                       UNICAST3.getSimpleName(), NAKACK2.getSimpleName(), RED.getSimpleName());
                throw new IllegalStateException(e);
            }
        }
    }

    protected static boolean isCallerRunsHandler(RejectedExecutionHandler h) {
        return h instanceof ThreadPoolExecutor.CallerRunsPolicy ||
          (h instanceof ShutdownRejectedExecutionHandler
            && ((ShutdownRejectedExecutionHandler)h).handler() instanceof ThreadPoolExecutor.CallerRunsPolicy);
    }
}