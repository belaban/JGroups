package org.jgroups.util;

import org.jgroups.Address;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Task which is seeded with an initial membership. Periodically executes a runnable (which e.g. sends a message) and
 * stops when responses from all members have been received, or the task is stopped.
 * @author Bela Ban
 * @since  4.0.5
 */
public class ResponseCollectorTask<T> extends ResponseCollector<T> {
    protected Consumer<ResponseCollectorTask<T>> periodic_task, finalizer_task; // run periodically until the task has been completed
    protected Future<?>                          runner;
    protected final Runnable                     stub=() -> periodic_task.accept(this);

    public ResponseCollectorTask() {
    }


    public ResponseCollectorTask(Collection<Address> members) {
        super(members);
    }

    public ResponseCollectorTask(Address... members) {
        super(members);
    }


    public ResponseCollectorTask<T> setPeriodicTask(Consumer<ResponseCollectorTask<T>> pt) {this.periodic_task=pt; return this;}
    public ResponseCollectorTask<T> setFinalizerTask(Consumer<ResponseCollectorTask<T>> r) {this.finalizer_task=r; return this;}
    public synchronized boolean     isDone()                     {return runner == null || runner.isDone();}

    public synchronized ResponseCollectorTask<T> start(TimeScheduler timer, long initial_delay, long interval) {
        if(hasAllResponses())
            return this;
        if(periodic_task != null && (runner == null || runner.isDone()))
            runner=timer.scheduleAtFixedRate(stub, initial_delay, interval, TimeUnit.MILLISECONDS);
        return this;
    }

    public synchronized ResponseCollectorTask<T> stop() {
        if(runner != null) {
            runner.cancel(true);
            runner=null;
            if(finalizer_task != null)
                finalizer_task.accept(this);
        }
        return this;
    }

    public boolean add(Address member, T data) {
        boolean retval;
        if((retval=super.add(member, data)) && hasAllResponses())
            stop();
        return retval;
    }

    public boolean retainAll(List<Address> members) {
        boolean retval=super.retainAll(members);
        if(retval && this.hasAllResponses())
            stop();
        return retval;
    }
}
