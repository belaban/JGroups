package org.jgroups.util;

/**
 * An object that can alter the state of a thread when it receives a callback from a {@link ThreadManager} notifying
 * it that the thread has been created or released from use.
 * 
 * @author Brian Stansberry
 * @version $Id: ThreadDecorator.java,v 1.1.2.1 2008/05/26 09:14:40 belaban Exp $
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
