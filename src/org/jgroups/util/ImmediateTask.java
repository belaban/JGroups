package org.jgroups.util;

/**
 * Task which is executed immediately and only one time
 * @author Bela Ban
 * @version $Id: ImmediateTask.java,v 1.1 2006/09/04 07:33:25 belaban Exp $
 */
public class ImmediateTask implements TimeScheduler.Task {
    boolean executed=false;
    Runnable r;

    public ImmediateTask(Runnable r) {
        this.r=r;
    }

    public boolean cancelled() {
        return executed;
    }

    public long nextInterval() {
        return 0;
    }

    public void run() {
        executed=true;
        if(r != null)
            r.run();
    }
}


