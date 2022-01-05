package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Implements storing of messages in a hashmap and sending of single messages and message batches. Most bundler
 * implementations will want to extend this class
 * @author Bela Ban
 * @since  4.0
 */
public abstract class BaseBundler implements Bundler {
    /** Keys are destinations, values are lists of Messages */
    protected final Map<Address,List<Message>>  msgs=new HashMap<>(24);
    protected TP                                transport;
    protected final ReentrantLock               lock=new ReentrantLock();
    protected @GuardedBy("lock") long           count;    // current number of bytes accumulated
    protected ByteArrayDataOutputStream         output;
    protected Log                               log;

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type=AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued until they are sent")
    protected int                               max_size=64000;

    @Property(description="The max number of elements in a bundler if the bundler supports size limitations",
      type=AttributeType.SCALAR)
    protected int                               capacity=16384;


    public int     getCapacity()       {return capacity;}
    public Bundler setCapacity(int c)  {this.capacity=c; return this;}
    public int     getMaxSize()        {return max_size;}
    public Bundler setMaxSize(int s)   {max_size=s; return this;}

    public void init(TP transport) {
        this.transport=transport;
        log=transport.getLog();
        output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
    }
    public void start() {}
    public void stop()  {}
    public void send(Message msg) throws Exception {}

    public void viewChange(View view) {
        // code removed (https://issues.jboss.org/browse/JGRP-2324)
    }

    /** Returns the total number of messages in the hashmap */
    @ManagedAttribute(description="The number of unsent messages in the bundler")
    public int size() {
        lock.lock();
        try {
            return msgs.values().stream().map(List::size).reduce(0, Integer::sum);
        }
        finally {
            lock.unlock();
        }
    }

    @ManagedAttribute(description="Size of the queue (if available")
    public int getQueueSize() {
        return -1;
    }

    /**
     * Sends all messages in the map. Messages for the same destination are bundled into a message list.
     * The map will be cleared when done.
     */
    @GuardedBy("lock") protected void sendBundledMessages() {
        for(Map.Entry<Address,List<Message>> entry: msgs.entrySet()) {
            List<Message> list=entry.getValue();
            if(list.isEmpty())
                continue;
            output.position(0);
            if(list.size() == 1)
                sendSingleMessage(list.get(0));
            else {
                Address dst=entry.getKey();
                sendMessageList(dst, list.get(0).getSrc(), list);
                if(transport.statsEnabled())
                    transport.getMessageStats().incrNumBatchesSent(1);
            }
            list.clear();
        }
        count=0;
    }


    protected void sendSingleMessage(final Message msg) {
        Address dest=msg.getDest();
        try {
            Util.writeMessage(msg, output, dest == null);
            transport.doSend(output.buffer(), 0, output.position(), dest);
            if(transport.statsEnabled())
                transport.getMessageStats().incrNumSingleMsgsSent(1);
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("SendFailure"),
                      transport.getAddress(), (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
    }



    protected void sendMessageList(final Address dest, final Address src, final List<Message> list) {
        try {
            Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, output, dest == null, transport.getId());
            transport.doSend(output.buffer(), 0, output.position(), dest);
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
        }
    }

    @GuardedBy("lock") protected void addMessage(Message msg, int size) {
        Address dest=msg.getDest();
        List<Message> tmp=msgs.computeIfAbsent(dest, k -> new ArrayList<>(16));
        tmp.add(msg);
        count+=size;
    }
}