package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.Runner;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


/**
 * Bundler which doesn't use locks but relies on CAS. There is 1 reader thread which gets unparked by (exactly one) writer
 * when the max size has been exceeded, or no other threads are sending messages.
 * @author Bela Ban
 * @since 4.0
 */
public class RingBufferBundlerLockless extends BaseBundler {
    protected Message[]                   buf;
    protected int                         read_index;
    protected volatile int                write_index=0;
    protected final AtomicInteger         tmp_write_index=new AtomicInteger(0);
    protected final AtomicInteger         write_permits; // number of permits to write tmp_write_index
    protected final AtomicInteger         size=new AtomicInteger(0); // number of messages to be read: read_index + count == write_index
    protected final AtomicInteger         num_threads=new AtomicInteger(0); // number of threads currently in send()
    protected final AtomicLong            accumulated_bytes=new AtomicLong(0); // total number of bytes of unread msgs
    protected final AtomicBoolean         unparking=new AtomicBoolean(false);
    protected Runner                      bundler_thread;
    protected static final String         THREAD_NAME="RingBufferBundlerLockless";
    protected final Runnable              run_function=this::readMessages;



    public RingBufferBundlerLockless() {
        this(1024);
    }


    public RingBufferBundlerLockless(int capacity) {
        buf=new Message[Util.getNextHigherPowerOfTwo(capacity)]; // for efficient % (mod) op
        this.write_permits=new AtomicInteger(buf.length);
    }

    public int readIndex()     {return read_index;}
    public int writeIndex()    {return write_index;}
    public int size()          {return size.get();}


    public void init(TP transport) {
        super.init(transport);
        bundler_thread=new Runner(transport.getThreadFactory(), THREAD_NAME, run_function, this::reset);
    }

    public void reset() {
        read_index=write_index=0;
        tmp_write_index.set(0);
        size.set(0);
    }

    public  void start() {
        bundler_thread.start();
    }

    public void stop() {
        bundler_thread.stop();
    }


    public void send(Message msg) throws Exception {
        if(msg == null)
            throw new IllegalArgumentException("message must not be null");
        num_threads.incrementAndGet();

        int tmp_index=getWriteIndex(); // decrements write_permits
        // System.out.printf("[%d] tmp_index=%d\n", Thread.currentThread().getId(), tmp_index);
        if(tmp_index == -1) {
            log.warn("buf is full (num_permits: %d, bundler: %s)\n", write_permits.get(), toString());
            num_threads.decrementAndGet();
            return;
        }

        buf[tmp_index]=msg;
        long acc_bytes=accumulated_bytes.addAndGet(msg.size());
        int current_threads=num_threads.decrementAndGet();
        boolean no_other_threads=current_threads == 0;

        boolean unpark=(acc_bytes >= transport.getMaxBundleSize() && accumulated_bytes.compareAndSet(acc_bytes, 0))
          ||  no_other_threads;

        // only 2 threads at a time should do this (1st cond and 2nd cond), so we have to reduce this to
        // 1 thread as advanceWriteIndex() is not thread safe
        if(unpark && unparking.compareAndSet(false, true)) {
            int num_advanced=advanceWriteIndex();
            size.addAndGet(num_advanced);
            if(num_advanced > 0) {
                Thread thread=bundler_thread.getThread();
                if(thread != null)
                    LockSupport.unpark(thread);
            }
            unparking.set(false);
        }
    }



    protected int getWriteIndex() {
        int permit=getPermitToWrite();
        if(permit < 0)
            return -1;

        // here we're guaranteed to have space available for writing, we now need to find the right one
        int next=tmp_write_index.getAndIncrement();
        int next_index=index(next);
        tmp_write_index.compareAndSet(next, next_index);
        return next_index;
    }

    protected int getPermitToWrite() {
        int remaining=write_permits.decrementAndGet();
        if(remaining < 0)
            write_permits.incrementAndGet();
        return remaining;
    }


    // Advance write_index up to tmp_write_index as long as no null msg is found
    protected int advanceWriteIndex() {
        int num=0, start=write_index;
        for(;;) {
            if(buf[start] == null)
                break;
            num++;
            start=index(start+1);
            if(start == tmp_write_index.get())
                break;
        }
        write_index=start;
        return num;
    }


    protected void readMessages() {
        _readMessages();
        LockSupport.park();
    }



    /** Read and send messages in range [read-index .. read-index+available_msgs-1] */
    protected int sendBundledMessages(final Message[] buf, final int read_index, int available_msgs) {
        int       max_bundle_size=transport.getMaxBundleSize();
        byte[]    cluster_name=transport.cluster_name.chars();
        int       start=read_index;
        int       sent_msgs=0;

        while(available_msgs > 0) {
            Message msg=buf[start];
            if(msg == null) {
                start=increment(start);
                available_msgs--;
                continue;
            }

            Address dest=msg.getDest();
            try {
                output.position(0);
                Util.writeMessageListHeader(dest, msg.getSrc(), cluster_name, 1, output, dest == null);

                // remember the position at which the number of messages (an int) was written, so we can later set the
                // correct value (when we know the correct number of messages)
                int size_pos=output.position() - Global.INT_SIZE;
                int num_msgs=marshalMessagesToSameDestination(dest, buf, start, available_msgs, max_bundle_size);
                sent_msgs+=num_msgs;
                if(num_msgs > 1) {
                    int current_pos=output.position();
                    output.position(size_pos);
                    output.writeInt(num_msgs);
                    output.position(current_pos);
                }
                transport.doSend(output.buffer(), 0, output.position(), dest);
                if(transport.statsEnabled())
                    transport.incrBatchesSent(num_msgs);
            }
            catch(Exception ex) {
                log.error("failed to send message(s)", ex);
            }

            available_msgs--;
            start=increment(start);
        }
        return sent_msgs;
    }

    public String toString() {
        return String.format("read-index=%d write-index=%d size=%d cap=%d", read_index, write_index, size.get(), buf.length);
    }

    public int _readMessages() {
        int available_msgs=size.get();
        if(available_msgs > 0) {
            int sent_msgs=sendBundledMessages(buf, read_index, available_msgs);
            read_index=index(read_index + sent_msgs);
            size.addAndGet(-sent_msgs);
            write_permits.addAndGet(sent_msgs);
            return sent_msgs;
        }
        return 0;
    }


    // Iterate through the following messages and find messages to the same destination (dest) and write them to output
    protected int marshalMessagesToSameDestination(Address dest, Message[] buf,
                                                   int start_index, int available_msgs, int max_bundle_size) throws Exception {
        int num_msgs=0, bytes=0;
        while(available_msgs > 0) {
            Message msg=buf[start_index];
            if(msg != null && Objects.equals(dest, msg.getDest())) {
                int msg_size=msg.size();
                if(bytes + msg_size > max_bundle_size)
                    break;
                bytes+=msg_size;
                num_msgs++;
                buf[start_index]=null;
                msg.writeToNoAddrs(msg.getSrc(), output, transport.getId());
            }
            available_msgs--;
            start_index=increment(start_index);
        }
        return num_msgs;
    }

    protected final int increment(int index) {return index+1 == buf.length? 0 : index+1;}
    protected final int index(int idx)     {return idx & (buf.length-1);}    // fast equivalent to %




    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
