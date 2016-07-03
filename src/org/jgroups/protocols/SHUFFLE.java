package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Reorders messages by buffering them and shuffling the result after TIMEOUT ms.
 *
 * @author Bela Ban
 *
 */
@Experimental
public class SHUFFLE extends Protocol {
    protected TimeScheduler       timer=null;
    protected final List<Message> up_msgs=new LinkedList<>();
    protected final List<Message> down_msgs=new LinkedList<>();
    protected Future<?>           task=null;


    @Property protected boolean   up=true;
    @Property protected boolean   down=false;

    @Property(description="max number of messages before we bundle")
    protected int                 max_size=10;

    @Property(description="max time (ms) before we pass the bundled messages up or down")
    protected long                max_time=1500L;


    public boolean isUp() {
        return up;
    }

    public void setUp(boolean up) {
        this.up=up;
    }

    public boolean isDown() {
        return down;
    }

    public void setDown(boolean down) {
        this.down=down;
    }

    public int getMaxSize() {
        return max_size;
    }

    public void setMaxSize(int max_size) {
        this.max_size=max_size;
    }

    public long getMaxTime() {
        return max_time;
    }

    public void setMaxTime(long max_time) {
        this.max_time=max_time;
    }

    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
    }

    public void stop() {
        super.stop();
        stopTask();
    }

    public void destroy() {
        super.destroy();
        stopTask();
    }

    public Object up(Event evt) {
        if(!up)
            return up_prot.up(evt);
        if(evt.getType() != Event.MSG)
            return up_prot.up(evt);
        Message msg=(Message)evt.getArg();
        synchronized(up_msgs) {
            up_msgs.add(msg);
        }
        if(up_msgs.size() >= max_size) {
            shuffleAndSendMessages();
        }
        else
            startTask();
        return null;
    }

    public void up(MessageBatch batch) {
        if(!up) {
            up_prot.up(batch);
            return;
        }

        synchronized(up_msgs) {
            for(Message msg: batch)
                up_msgs.add(msg);
        }
        batch.clear();

        if(up_msgs.size() >= max_size)
            shuffleAndSendMessages();
        else
            startTask();
    }

    public Object down(Event evt) {
        if(!down)
            return down_prot.down(evt);
        if(evt.getType() != Event.MSG)
            return down_prot.down(evt);
        Message msg=(Message)evt.getArg();
        synchronized(down_msgs) {
            down_msgs.add(msg);
        }
        if(down_msgs.size() >= max_size) {
            shuffleAndSendMessages();
        }
        else
            startTask();
        return null;
    }

    protected synchronized void startTask() {
        if(task == null || task.isDone() || task.isCancelled()) {
            task=timer.schedule((Runnable)this::shuffleAndSendMessages, max_time, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void stopTask() {
        if(task != null)
            task.cancel(true);
    }

    private void shuffleAndSendMessages() {
        synchronized(up_msgs) {
            if(!up_msgs.isEmpty()) {
                Collections.shuffle(up_msgs);
                up_msgs.stream().filter(msg -> up_prot != null).forEach(msg -> up_prot.up(new Event(Event.MSG, msg)));
                up_msgs.clear();
            }
        }

        synchronized(down_msgs) {
            if(!down_msgs.isEmpty()) {
                Collections.shuffle(down_msgs);
                for(Message msg: down_msgs)
                    down_prot.down(new Event(Event.MSG, msg));
                down_msgs.clear();
            }
        }
    }


}
