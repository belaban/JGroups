package org.jgroups.util;

/**
 * An object that can alter the state of a thread when it receives a callback from a {@link ThreadManager} notifying
 * it that the thread has been created or released from use.
 * 
 * @author Brian Stansberry
 */
public interface ThreadDecorator {
   /**
    * Notification that <code>thread</code> has just been created.
    * @param thread the thread
    */
   void threadCreated(Thread thread);
   
   /**
    * Notification that <code>thread</code> has just been released from use
    * (e.g. returned to a thread pool after executing a task).
    * @param thread the thread
    */
   void threadReleased(Thread thread);
}
