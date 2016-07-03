package org.jgroups.blocks.executor;

/**
 * @author wburns
 */
public interface ExecutorNotification {
    void resultReturned(Object obj);
    
    void throwableEncountered(Throwable t);
    
    void interrupted(Runnable runnable);
}
