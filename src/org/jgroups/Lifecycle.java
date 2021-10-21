package org.jgroups;

/**
 * Interface that defines lifecycle methods. Used by protocols and components
 * @author Bela Ban
 * @since  5.2
 */
public interface Lifecycle {

    /**
     * Called after an instance has been created and before it is started.
     * @exception Exception Thrown if the instance cannot be initialized successfully.
     */
    default void init() throws Exception {}

    /**
     * This method is called after an instance has been initialized. Starts work.
     * @exception Exception Thrown if the instance cannot be started successfully.
     */
    default void start() throws Exception {}

    /**
     * Called before an instance is stopped; stops work.
     */
    default void stop() {}


    /**
     * Called after an instance has been stopped. Cleans up resources
     */
    default void destroy() {}

}
