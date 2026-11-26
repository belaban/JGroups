
package org.jgroups.conf;

import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolHook;

import java.util.List;


public interface ProtocolStackConfigurator extends ProtocolHook {
    String                      getProtocolStackString();
    List<ProtocolConfiguration> getProtocolStack();

    /**
     * Invoked after each {@link Protocol} is instantiated and before {@link Protocol#init()} is invoked.
     *
     * @param prot The protocol that was created.
     * @throws Exception If any exception occurred during method invocation.
     */
    @Override
    default void afterCreation(Protocol prot) throws Exception {
        //no-op by default
    }
}
