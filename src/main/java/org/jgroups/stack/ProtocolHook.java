package org.jgroups.stack;

/**
 * Provides hook(s) that are called when a protocol has been created
 * @author Bela Ban
 * @since  3.6.7
 */
public interface ProtocolHook {
    /**
     * Called after all protocols have been created, connected and its attributes set, but
     * before {@link Protocol#init()} is called. The order of calling the hooks is from bottom to top protocol.
     * @param prot The protocol that was created.
     * @throws Exception Thrown is the method failed.
     */
    void afterCreation(Protocol prot) throws Exception;
}
