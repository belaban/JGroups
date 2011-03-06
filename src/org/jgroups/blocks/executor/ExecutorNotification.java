package org.jgroups.blocks.executor;

/**
 * @author wburns
 */
public interface ExecutorNotification {
    public void resultReturned(Object obj);
    
    public void throwableEncountered(Throwable t);
    
    public void interrupted(Runnable runnable);
}
