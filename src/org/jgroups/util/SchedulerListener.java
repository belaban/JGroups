// $Id: SchedulerListener.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


public interface SchedulerListener {
    void started(Runnable   r);
    void stopped(Runnable   r);
    void suspended(Runnable r);
    void resumed(Runnable   r);
}
