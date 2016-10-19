package org.jgroups.protocols;

import org.jgroups.annotations.ManagedAttribute;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Class which has all the stats about received/sent messages etc (in TP)
 * @author Bela Ban
 * @since  4.0
 */
public class MsgStats {
    protected final LongAdder     num_msgs_sent=new LongAdder();
    protected final LongAdder     num_msgs_received=new LongAdder();
    protected final LongAdder     num_single_msgs_sent=new LongAdder();
    protected final LongAdder     num_oob_msgs_received=new LongAdder();
    protected final LongAdder     num_internal_msgs_received=new LongAdder();
    protected final LongAdder     num_batches_sent=new LongAdder();
    protected final LongAdder     num_batches_received=new LongAdder();
    protected final LongAdder     num_bytes_sent=new LongAdder();
    protected final LongAdder     num_bytes_received=new LongAdder();
    protected final AtomicInteger num_rejected_msgs=new AtomicInteger(0);
    protected final AtomicInteger num_threads_spawned=new AtomicInteger(0);


    @ManagedAttribute(description="Number of messages sent")
    public long     getNumMsgsSent()                   {return num_msgs_sent.sum();}
    public MsgStats incrNumMsgsSent(int d)             {num_msgs_sent.add(d); return this;}

    @ManagedAttribute(description="Number of regular messages received")
    public long     getNumMsgsReceived()               {return num_msgs_received.sum();}
    public MsgStats incrNumMsgsReceived(int d)         {num_msgs_received.add(d); return this;}

    @ManagedAttribute(description="Number of OOB messages received. This value is included in num_msgs_received.")
    public long     getNumOOBMsgsReceived()            {return num_oob_msgs_received.sum();}
    public MsgStats incrNumOOBMsgsReceived(int d)      {num_oob_msgs_received.add(d); return this;}

    @ManagedAttribute(description="Number of internal messages received. This value is included in num_msgs_received.")
    public long     getNumInternalMsgsReceived()       {return num_internal_msgs_received.sum();}
    public MsgStats incrNumInternalMsgsReceived(int d) {num_internal_msgs_received.add(d); return this;}

    @ManagedAttribute(description="Number of single messages sent")
    public long     getNumSingleMsgsSent()             {return num_single_msgs_sent.sum();}
    public MsgStats incrNumSingleMsgsSent(int d)       {num_single_msgs_sent.add(d); return this;}

    @ManagedAttribute(description="Number of message batches sent")
    public long     getNumBatchesSent()                {return num_batches_sent.sum();}
    public MsgStats incrNumBatchesSent(int d)          {num_batches_sent.add(d); return this;}

    @ManagedAttribute(description="Number of message batches received")
    public long     getNumBatchesReceived()            {return num_batches_received.sum();}
    public MsgStats incrNumBatchesReceived(int d)      {num_batches_received.add(d); return this;}

    @ManagedAttribute(description="Number of bytes sent")
    public long     getNumBytesSent()                  {return num_bytes_sent.sum();}
    public MsgStats incrNumBytesSent(int d)            {num_bytes_sent.add(d); return this;}

    @ManagedAttribute(description="Number of bytes received")
    public long     getNumBytesReceived()              {return num_bytes_received.sum();}
    public MsgStats incrNumBytesReceived(int d)        {num_bytes_received.add(d); return this;}

    @ManagedAttribute(description="Number of dropped messages that were rejected by the thread pool")
    public int      getNumRejectedMsgs()               {return num_rejected_msgs.get();}
    public MsgStats incrNumRejectedMsgs(int d)         {num_rejected_msgs.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of threads spawned as a result of thread pool rejection")
    public int      getNumThreadsSpawned()             {return num_threads_spawned.get();}
    public MsgStats incrNumThreadsSpawned(int d)       {num_threads_spawned.addAndGet(d); return this;}


    public MsgStats reset() {
        Stream.of(num_msgs_sent, num_msgs_received, num_single_msgs_sent, num_oob_msgs_received,
                  num_internal_msgs_received, num_batches_sent, num_batches_received, num_bytes_sent,num_bytes_received)
          .forEach(LongAdder::reset);
        Stream.of(num_rejected_msgs,num_threads_spawned).forEach(ai -> ai.set(0));
        return this;
    }
}
