package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.stack.Protocol;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Forwards messages in FIFO order to a destination. Uses IDs to prevent duplicates. Used by
 * {@link org.jgroups.protocols.FORWARD_TO_COORD}.
 * @author Bela Ban
 * @since 3.3
 */
public class ForwardQueue {
    protected Protocol                          up_prot, down_prot;

    protected Address                           local_addr;

    /** Maintains messages forwarded to the target which which no ack has been received yet.
     *  Needs to be sorted so we can resend them in the right order */
    protected final NavigableMap<Long,Message>  forward_table=new ConcurrentSkipListMap<>();

    protected final Lock                        send_lock=new ReentrantLock();

    protected final Condition                   send_cond=send_lock.newCondition();

    /** Set when we block all sending threads to resend all messages from forward_table */
    protected volatile boolean                  flushing=false;

    protected volatile boolean                  running=true;

    /** Keeps track of the threads sending messages */
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);

    // Maintains received seqnos, so we can weed out dupes
    protected final ConcurrentMap<Address,BoundedHashMap<Long,Long>> delivery_table=Util.createConcurrentMap();

    protected volatile Flusher                  flusher;

    /** Used for each resent message to wait until the message has been received */
    protected final Promise<Long>               ack_promise=new Promise<>();

    protected final Log                         log;

    /** Size of the set to store received seqnos (for duplicate checking) */
    protected int                               delivery_table_max_size=500;





    public ForwardQueue(Log log) {
        this.log=log;
    }

    public Protocol getUpProt()                           {return up_prot;}
    public void     setUpProt(Protocol up_prot)           {this.up_prot=up_prot;}

    public Protocol getDownProt()                         {return down_prot;}
    public void     setDownProt(Protocol down_prot)       {this.down_prot=down_prot;}

    public Address  getLocalAddr()                        {return local_addr;}
    public void     setLocalAddr(Address local_addr)      {this.local_addr=local_addr;}

    public int      getDeliveryTableMaxSize()             {return delivery_table_max_size;}
    public void     setDeliveryTableMaxSize(int max_size) {this.delivery_table_max_size=max_size;}


    /** Total size of all queues of the delivery table */
    public int deliveryTableSize() {
        int retval=0;
        for(BoundedHashMap<Long,Long> val: delivery_table.values())
            retval+=val.size();
        return retval;
    }


    public void start() {
        running=true;
    }

    public void stop() {
        running=false;
        unblockAll();
        stopFlusher();
        forward_table.clear();
    }


    public void send(long id, Message msg) {
        if(flushing)
            block();

        in_flight_sends.incrementAndGet();
        try {
            forward_table.put(id, msg);
            if(running && !flushing)
                down_prot.down(msg);
        }
        finally {
            in_flight_sends.decrementAndGet();
        }
    }

    public void receive(long id, Message msg) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver message " + "::" + id);
            return;
        }
        if(!canDeliver(sender, id)) {
            if(log.isWarnEnabled())
                log.warn(local_addr + ": dropped duplicate message " + sender + "::" + id);
            return;
        }
        if(log.isTraceEnabled())
            log.trace(local_addr + ": delivering " + sender + "::" + id);
        up_prot.up(msg);
    }

    public void flush(Address new_target, final List<Address> mbrs) {
        delivery_table.keySet().retainAll(mbrs);
        if(new_target != null) {
            stopFlusher();
            startFlusher(new_target); // needs to be done in the background, to prevent blocking if down() would block
        }
    }

    public void ack(long id) {
        forward_table.remove(id);
        ack_promise.setResult(id);  // lock acquition, but most of the times it is uncontended, so this is not a cost
    }


    public int size() {return forward_table.size();}


    protected void doFlush(final Address new_target) throws InterruptedException {
        // wait until all threads currently sending messages have returned (new threads after flushing=true) will block
        // flushing is set to true in startFlusher()
        while(flushing && running) {
            if(in_flight_sends.get() == 0)
                break;
            Thread.sleep(100);
        }

        send_lock.lockInterruptibly();
        try {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": target changed to " + new_target);
            flushMessagesInForwardTable(new_target);
        }
        finally {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing completed");
            flushing=false;
            send_cond.signalAll();
            send_lock.unlock();
        }
    }

    /**
     * Sends all messages currently in forward_table to the new target (changing the dest field).
     * This needs to be done, so the underlying reliable unicast protocol (e.g. UNICAST) adds these messages
     * to its retransmission mechanism<br/>
     * Note that we need to resend the messages in order of their seqnos ! We also need to prevent other message
     * from being inserted until we're done, that's why there's synchronization.<br/>
     * Access to the forward_table doesn't need to be synchronized as there won't be any insertions during flushing
     * (all down-threads are blocked)
     */
    protected void flushMessagesInForwardTable(Address target) {
        // for forwarded messages, we need to receive the forwarded message from the coordinator, to prevent this case:
        // - V1={A,B,C}
        // - A crashes
        // - C installs V2={B,C}
        // - C forwards messages 3 and 4 to B (the new coord)
        // - B drops 3 because its view is still V1
        // - B installs V2
        // - B receives message 4 and broadcasts it
        // ==> C's message 4 is delivered *before* message 3 !
        // ==> By resending 3 until it is received, then resending 4 until it is received, we make sure this won't happen
        // (see https://issues.jboss.org/browse/JGRP-1449)

        // Forward the first entry and wait for the ack
        Map.Entry<Long,Message> first=forward_table.firstEntry();
        if(first == null)
            return;
        Long key=first.getKey();
        Message val=first.getValue();

        while(flushing && running && !forward_table.isEmpty()) {
            Message forward_msg=val.copy(true, true)
              .setDest(target).setFlag(Message.Flag.DONT_BUNDLE);
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing (forwarding) " + "::" + key + " to target " + target);
            ack_promise.reset();
            down_prot.down(forward_msg);
            Long ack=ack_promise.getResult(500);
            if((Objects.equals(ack, key)) || !forward_table.containsKey(key))
                break;
        }

        for(Map.Entry<Long,Message> entry: forward_table.entrySet()) {
            key=entry.getKey();
            val=entry.getValue();
            if(flushing && running) {
                Message forward_msg=val.copy(true, true)
                  .setDest(target).setFlag(Message.Flag.DONT_BUNDLE);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (forwarding) " + "::" + key + " to target " + target);
                down_prot.down(forward_msg);
            }
        }
    }


    /**
     * Checks if seqno has already been received from sender. This weeds out duplicates.
     * Note that this method is never called concurrently for the same sender.
     */
    protected boolean canDeliver(Address sender, long seqno) {
        BoundedHashMap<Long,Long> seqno_set=delivery_table.get(sender);
        if(seqno_set == null) {
            seqno_set=new BoundedHashMap<>(delivery_table_max_size);
            BoundedHashMap<Long,Long> existing=delivery_table.put(sender,seqno_set);
            if(existing != null)
                seqno_set=existing;
        }
        return seqno_set.add(seqno, seqno);
    }

    protected void block() {
        send_lock.lock();
        try {
            while(flushing && running) {
                try {
                    send_cond.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            send_lock.unlock();
        }
    }

    protected void unblockAll() {
        flushing=false;
        send_lock.lock();
        try {
            send_cond.signalAll();
            ack_promise.setResult(null);
        }
        finally {
            send_lock.unlock();
        }
    }


    protected synchronized void startFlusher(final Address new_coord) {
        if(flusher == null || !flusher.isAlive()) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing started");
            // causes subsequent message sends (broadcasts and forwards) to block (https://issues.jboss.org/browse/JGRP-1495)
            flushing=true;

            flusher=new Flusher(new_coord);
            flusher.setName("Flusher");
            flusher.start();
        }
    }

    protected void stopFlusher() {
        flushing=false;
        Thread tmp=flusher;

        while(tmp != null && tmp.isAlive()) {
            tmp.interrupt();
            ack_promise.setResult(null);
            try {
                tmp.join();
            }
            catch(InterruptedException e) {
            }
        }
    }



    protected class Flusher extends Thread {
        protected final Address new_coord;

        public Flusher(Address new_coord) {
            this.new_coord=new_coord;
        }

        public void run() {
            try {
                doFlush(new_coord);
            }
            catch (InterruptedException e) {
            }
        }
    }
}
