package org.jgroups.blocks;

/**
 * Can be used by {@link RpcDispatcher} / {@link MethodCall} to invoke a method against a given target object
 * @author Bela Ban
 * @since  4.1.0
 */
public interface MethodInvoker {

    /**
     * An implementation invokes a method associated with a given ID and the given args against a target object
     * @param target The object against which to invoke the method
     * @param method_id The ID of the method. The implementation must assign unique IDs and associate them somehow
     *                  with a method to invoke
     * @param args The arguments of the invocation
     * @return The result. It may be null if a method returns void
     * @throws Exception Thrown if the invocation threw an exception. The real exception may be wrapped in an
     * {@link java.lang.reflect.InvocationTargetException}.
     */
    Object invoke(Object target, short method_id, Object[] args) throws Exception;
}
