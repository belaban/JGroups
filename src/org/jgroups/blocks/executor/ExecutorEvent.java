package org.jgroups.blocks.executor;

import org.jgroups.Event;

/**
 * Defines an event class for the execution of an entity.
 * 
 * @author wburns
 */
public class ExecutorEvent extends Event {
    
    public static final int TASK_SUBMIT = 1024;     // arg = Runnable (Serializable)
    public static final int CONSUMER_READY = 1025;  // arg = null
    public static final int TASK_COMPLETE = 1026;   // arg = [Runnable, Throwable] or Runnable
    public static final int TASK_CANCEL = 1027;     // arg = [Runnable, boolean]
    public static final int ALL_TASK_CANCEL = 1028; // arg = [Set<Runnable>, boolean]

    /**
     * @param type
     * @param arg
     */
    public ExecutorEvent(int type, Object arg) {
        super(type, arg);
    }
}
