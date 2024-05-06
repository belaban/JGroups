package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AsciiString;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Intercepts individual up messages and creates batches from them, passing the batches up. Used by unit tests, not
 * meant to be used in production.
 * @author Bela Ban
 * @since  3.5
 */
@MBean(description="Intercepts single messages and passes them up as batches")
public class MAKE_BATCH extends Protocol {
    @Property(description="handle multicast messages")
    protected boolean multicasts=false;

    @Property(description="handle unicast messages")
    protected boolean unicasts=false;

    @Property(description="Do not add OOB messages to a batch if true")
    protected boolean skip_oob=true;

    @Property(description="Time to sleep (in ms) from the reception of the first message to sending a batch up",
      type=AttributeType.TIME)
    protected long sleep_time=100;

    // all maps have sender and msg list pairs
    protected final Map<Address,List<Message>> reg_map_mcast=new ConcurrentHashMap<>();
    protected final Map<Address,List<Message>> reg_map_ucast=new ConcurrentHashMap<>();

    protected final Map<Address,List<Message>> oob_map_mcast=new ConcurrentHashMap<>();
    protected final Map<Address,List<Message>> oob_map_ucast=new ConcurrentHashMap<>();

    protected TimeScheduler                     timer;
    protected AsciiString                       cluster_name;
    protected Future<?>                         batcher;

    public MAKE_BATCH multicasts(boolean flag) {this.multicasts=flag; return this;}
    public MAKE_BATCH unicasts(boolean flag)   {this.unicasts=flag;   return this;}
    public MAKE_BATCH sleepTime(long time)     {this.sleep_time=time; return this;}
    public boolean    skipOOB()                {return skip_oob;}
    public MAKE_BATCH skipOOB(boolean s)       {this.skip_oob=s; return this;}


    public void start() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        startBatcher();
    }

    public void stop() {
        super.stop();
        stopBatcher();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
                cluster_name=new AsciiString((String)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Message msg) {
        if(msg.isFlagSet(Message.Flag.OOB) && skip_oob)
            return up_prot.up(msg);

        if((msg.getDest() == null && multicasts) || (msg.getDest() != null && unicasts)) {
            queue(msg);
            return null;
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg.isFlagSet(Message.Flag.OOB) && skip_oob) {
                up_prot.up(msg);
                it.remove();
                continue;
            }
            if((msg.dest() == null && multicasts) || (msg.dest() != null && unicasts)) {
                queue(msg);
                it.remove();
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void queue(Message msg) {
        Address dest=msg.getDest();
        Map<Address,List<Message>> map;
        if(dest == null)
            map=msg.isFlagSet(Message.Flag.OOB)? oob_map_mcast : reg_map_mcast;
        else
            map=msg.isFlagSet(Message.Flag.OOB)? oob_map_ucast : reg_map_ucast;

        List<Message> list=map.computeIfAbsent(msg.src(), k -> new ArrayList<>());
        list.add(msg);
    }

    public synchronized MAKE_BATCH startBatcher() {
        if(timer == null)
            timer=getTransport().getTimer();
        if(batcher == null || batcher.isDone())
            batcher=timer.scheduleWithFixedDelay(new Batcher(), sleep_time, sleep_time, TimeUnit.MILLISECONDS);
        return this;
    }

    protected synchronized MAKE_BATCH stopBatcher() {
        if(batcher != null) {
            batcher.cancel(true);
            batcher=null;
        }
        return this;
    }

    protected class Batcher implements Runnable {

        public void run() {
            List<MessageBatch> batches=new ArrayList<>();

            synchronized(oob_map_mcast) {
                for(Map.Entry<Address,List<Message>> entry: oob_map_mcast.entrySet()) {
                    MessageBatch batch=new MessageBatch(null, entry.getKey(), cluster_name, true, entry.getValue()).mode(MessageBatch.Mode.OOB);
                    batches.add(batch);
                }
                oob_map_mcast.clear();
            }

            synchronized(oob_map_ucast) {
                for(Map.Entry<Address,List<Message>> entry: oob_map_ucast.entrySet()) {
                    MessageBatch batch=new MessageBatch(local_addr, entry.getKey(), cluster_name, false, entry.getValue()).mode(MessageBatch.Mode.OOB);
                    batches.add(batch);
                }
                oob_map_ucast.clear();
            }

            synchronized(reg_map_mcast) {
                for(Map.Entry<Address,List<Message>> entry: reg_map_mcast.entrySet()) {
                    MessageBatch batch=new MessageBatch(null, entry.getKey(), cluster_name, true, entry.getValue()).mode(MessageBatch.Mode.REG);
                    batches.add(batch);
                }
                reg_map_mcast.clear();
            }

            synchronized(reg_map_ucast) {
                for(Map.Entry<Address,List<Message>> entry: reg_map_ucast.entrySet()) {
                    MessageBatch batch=new MessageBatch(local_addr, entry.getKey(), cluster_name, false, entry.getValue()).mode(MessageBatch.Mode.REG);
                    batches.add(batch);
                }
                reg_map_ucast.clear();
            }
            batches.stream().filter(batch -> !batch.isEmpty()).forEach(batch -> up_prot.up(batch));
        }
    }
}
