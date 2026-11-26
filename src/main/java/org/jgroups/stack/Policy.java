package org.jgroups.stack;

/**
 * A policy implementation checks that a condition is met or throws an exception if not. A condition could be for
 * example the check if a given protocol is present, an attribute is within a given range etc.
 * <br/>
 * Policies are called after {@link Protocol#init()}, so the protocol is full initialized (all attrs and components
 * are set).
 * @author Bela Ban
 * @since  5.2.14
 */
public interface Policy {
    /**
     * Checks that a condition is met in a given protocol
     * @param prot The protocol in which the policy is run
     * @throws Exception Thrown if the condition is not met
     */
    void check(Protocol prot) throws Exception;
}
