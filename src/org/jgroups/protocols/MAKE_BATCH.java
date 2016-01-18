package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.AbstractProtocol;
import org.jgroups.util.AsciiString;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Intercepts individual up messages and creates batches from them, passing the batches up. Used by unit tests, not
 * meant to be used in production.
 * @author Bela Ban
 * @since  3.5
 */
@MBean(description="Intercepts single messages and passes them up as batches")
public class MAKE_BATCH extends AbstractProtocol {
    @Property(description="handle multicast messages")
    protected boolean multicasts=false;

    @Property(description="handle unicast messages")
    protected boolean unicasts=false;

    @Property(description="Time to sleep (in ms) from the reception of the first message to sending a batch up")
    protected long sleep_time=100;

    // all maps have sender and msg list pairs
    protected final Map<Address,List<Message>> reg_map_mcast=new HashMap<>();
    protected final Map<Address,List<Message>> reg_map_ucast=new HashMap<>();

    protected final Map<Address,List<Message>> oob_map_mcast=new HashMap<>();
    protected final Map<Address,List<Message>> oob_map_ucast=new HashMap<>();

    protected TimeScheduler                     timer;
    protected AsciiString                       cluster_name;
    protected Address                           local_addr;
    protected Future<?>                         batcher;


    public MAKE_BATCH multicasts(boolean flag) {this.multicasts=flag; return this;}
    public MAKE_BATCH unicasts(boolean flag)   {this.unicasts=flag;   return this;}
    public MAKE_BATCH sleepTime(long time)     {this.sleep_time=time; return this;}


    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        startBatcher();
    }

    public void destroy() {
        super.destroy();
        stopBatcher();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster_name=new AsciiString((String)evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            if(msg.isFlagSet(Message.Flag.OOB) && msg.isFlagSet(Message.Flag.INTERNAL))
                return up_prot.up(evt);

            if((msg.dest() == null && multicasts) || (msg.dest() != null && unicasts)) {
                queue(msg);
                return null;
            }
        }
        return up_prot.up(evt);
    }

    protected void queue(Message msg) {
        Address dest=msg.dest();
        Map<Address,List<Message>> map;
        if(dest == null)
            map=msg.isFlagSet(Message.Flag.OOB)? oob_map_mcast : reg_map_mcast;
        else
            map=msg.isFlagSet(Message.Flag.OOB)? oob_map_ucast : reg_map_ucast;

        Address sender=msg.src();
        synchronized(map) {
            List<Message> list=map.get(sender);
            if(list == null)
                map.put(sender, list=new ArrayList<>());
            list.add(msg);
        }
    }

    public synchronized void startBatcher() {
        if(timer == null)
            timer=getTransport().getTimer();
        if(batcher == null || batcher.isDone())
            batcher=timer.scheduleWithFixedDelay(new Batcher(), sleep_time, sleep_time, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopBatcher() {
        if(batcher != null) {
            batcher.cancel(true);
            batcher=null;
        }
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

            for(MessageBatch batch: batches) {
                if(!batch.isEmpty()) {
                    // if(UUID.get(batch.sender()).equals("A"))
                       // System.out.println("**** sending up batch " + batch + ", hdrs: " + batch.printHeaders());
                    up_prot.up(batch);
                }
            }
        }
    }
}
