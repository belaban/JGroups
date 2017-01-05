package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;


/**
 * Reorders messages by buffering them and shuffling the result after TIMEOUT ms.
 * @author Bela Ban
 *
 */
public class SHUFFLE extends Protocol {
    protected TimeScheduler  timer;
    protected List<Message>  up_msgs;
    protected List<Message>  down_msgs;
    protected final Lock     up_lock=new ReentrantLock();
    protected final Lock     down_lock=new ReentrantLock();
    protected Future<?>      task;


    @Property(description="Reorder up messages and message batches")
    protected boolean             up=true;

    @Property(description="Reorder down messages and message batches")
    protected boolean             down;

    @Property(description="max number of messages before we reorder queued messages and send them up")
    protected int                 max_size=10;

    @Property(description="max time (in millis) before we pass the reordered messages up or down")
    protected long                max_time=1500L;


    public boolean isUp()                    {return up;}
    public SHUFFLE setUp(boolean up)         {this.up=up; return this;}
    public boolean isDown()                  {return down;}
    public SHUFFLE setDown(boolean down)     {this.down=down; return this;}
    public int     getMaxSize()              {return max_size;}
    public SHUFFLE setMaxSize(int max_size)  {this.max_size=max_size; return this;}
    public long    getMaxTime()              {return max_time;}
    public SHUFFLE setMaxTime(long max_time) {this.max_time=max_time; return this;}


    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        up_msgs=new ArrayList<>(max_size);
        down_msgs=new ArrayList<>(max_size);
    }

    public void stop() {
        super.stop();
        stopTask();
    }

    public void destroy() {
        super.destroy();
        stopTask();
    }

    public Object down(Message msg) {
        if(!down)
            return down_prot.down(msg);
        add(down_msgs, msg, down_lock, m -> down_prot.down(m));
        return null;
    }

    public Object up(Message msg) {
        if(!up)
            return up_prot.up(msg);
        add(up_msgs, msg, up_lock, m -> up_prot.up(msg));
        return null;
    }

    public void up(MessageBatch batch) {
        if(!up) {
            up_prot.up(batch);
            return;
        }

        if(batch.size() > 1) {
            shuffle(batch);
            if(!batch.isEmpty())
                up_prot.up(batch);
        }
        else
            add(up_msgs, batch, up_lock, m -> up_prot.up(m));
    }


    protected static void shuffle(MessageBatch batch) {
        Message[] msgs=batch.array();
        Util.shuffle(msgs, 0, batch.index());
    }



    protected synchronized void startTask() {
        if(task == null || task.isDone() || task.isCancelled())
            task=timer.schedule(() -> {
                                    reorderAndSend(up_msgs,   up_lock,   msg -> up_prot.up(msg));
                                    reorderAndSend(down_msgs, down_lock, msg -> down_prot.down(msg));
                                },
                                max_time, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopTask() {
        if(task != null)
            task.cancel(true);
    }

    protected SHUFFLE add(List<Message> queue, Message msg, Lock lock, Consumer<Message> send_function) {
        lock.lock();
        try {
            queue.add(msg);
            if(queue.size() >= max_size)
                reorderAndSend(queue, lock, send_function); // clears queue
            else
                startTask();
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    protected SHUFFLE add(List<Message> queue, MessageBatch batch, Lock lock, Consumer<Message> send_function) {
        lock.lock();
        try {
            for(Message msg: batch)
                queue.add(msg); // queue can become a bit larger as a result of adding the entire batch
            if(queue.size() >= max_size)
                reorderAndSend(queue, lock, send_function); // clears queue
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    protected SHUFFLE reorderAndSend(List<Message> list, final Lock lock, Consumer<Message> send_function) {
        lock.lock();
        try {
            Collections.shuffle(list);
            list.forEach(send_function);
            list.clear();
            return this;
        }
        finally {
            lock.unlock();
        }
    }


}
