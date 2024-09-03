package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.jgroups.conf.AttributeType.BYTES;
import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * Class which has all the stats about received/sent messages etc. (in TP)
 * @author Bela Ban
 * @since  4.0
 */
public class MsgStats {
    @Property(description="Whether metrics should be logged")
    protected boolean             enabled;

    @ManagedAttribute(description="Number of unicast messages sent",type=SCALAR)
    protected final LongAdder     num_ucasts_sent=new LongAdder();
    @ManagedAttribute(description="Number of unicast messages received",type=SCALAR)
    protected final LongAdder     num_ucasts_received=new LongAdder();

    @ManagedAttribute(description="Number of multicast messages sent",type=SCALAR)
    protected final LongAdder     num_mcasts_sent=new LongAdder();
    @ManagedAttribute(description="Total number of multicast messages received",type=SCALAR)
    protected final LongAdder     num_mcasts_received=new LongAdder();

    @ManagedAttribute(description="Number of single messages sent (by the bundler)",type=SCALAR)
    protected final LongAdder     num_single_msgs_sent=new LongAdder();
    @ManagedAttribute(description="Number of single messages received (by the transport)",type=SCALAR)
    protected final LongAdder     num_single_msgs_received=new LongAdder();

    @ManagedAttribute(description="Number of batches sent (by the bundler)",type=SCALAR)
    protected final LongAdder     num_batches_sent=new LongAdder();
    @ManagedAttribute(description="Number of batches received (by the transport)",type=SCALAR)
    protected final LongAdder     num_batches_received=new LongAdder();

    /** The average number of messages in a received {@link MessageBatch} */
    @ManagedAttribute(description="Returns the average batch size of received batches")
    protected final AverageMinMax avg_batch_size=new AverageMinMax(1024);

    @ManagedAttribute(description="Number of multicast bytes sent",type=BYTES)
    protected final LongAdder     num_mcast_bytes_sent=new LongAdder();
    @ManagedAttribute(description="Number of multicast bytes received",type=BYTES)
    protected final LongAdder     num_mcast_bytes_received=new LongAdder();

    @ManagedAttribute(description="Number of unicast bytes sent",type=BYTES)
    protected final LongAdder     num_ucast_bytes_sent=new LongAdder();
    @ManagedAttribute(description="Number of unicast bytes received",type=BYTES)
    protected final LongAdder     num_ucast_bytes_received=new LongAdder();

    public boolean  enabled()                   {return enabled;}
    public MsgStats enable(boolean b)           {enabled=b; return this;}

    @ManagedAttribute(description="Number of messages sent (mcasts and ucasts sent)",type=SCALAR)
    public long     getNumMsgsSent()            {return num_mcasts_sent.sum() + num_ucasts_sent.sum();}

    @ManagedAttribute(description="Number of messages received (mcasts and ucasts received)",type=SCALAR)
    public long     getNumMsgsReceived()        {return num_mcasts_received.sum() + num_ucasts_received.sum();}

    public AverageMinMax getAvgBatchSize()      {return avg_batch_size;}

    @ManagedAttribute(description="Total number of bytes sent (unicast + multicast bytes)",type=BYTES)
    public long     getNumBytesSent()           {return num_mcast_bytes_sent.sum() + num_ucast_bytes_sent.sum();}

    @ManagedAttribute(description="Total number of bytes received (unicast + multicast)",type=BYTES)
    public long     getNumBytesReceived()       {return num_mcast_bytes_received.sum() + num_ucast_bytes_received.sum();}

    public long     getNumUcastsSent()          {return num_ucasts_sent.sum();}

    public long     getNumMcastsSent()          {return num_mcasts_sent.sum();}

    public long     getNumUcastsReceived()      {return num_ucasts_received.sum();}

    public long     getNumMcastsReceived()      {return num_mcasts_received.sum();}

    public long     getNumSingleMsgsSent()      {return num_single_msgs_sent.sum();}
    public MsgStats incrNumSingleMsgsSent()     {if(enabled) num_single_msgs_sent.increment(); return this;}

    public long     getNumBatchesSent()         {return num_batches_sent.sum();}
    public MsgStats incrNumBatchesSent()        {if(enabled) num_batches_sent.increment(); return this;}
    public MsgStats incrNumBatchesSent(int n)   {if(enabled) num_batches_sent.add(n); return this;}

    public long     getNumBatchesReceived()     {return num_batches_received.sum();}

    public long     getNumUcastBytesSent()      {return num_ucast_bytes_sent.sum();}

    public long     getNumMcastBytesSent()      {return num_mcast_bytes_sent.sum();}

    public long     getNumUcastBytesReceived()  {return num_ucast_bytes_received.sum();}

    public long     getNumMcastBytesReceived()  {return num_mcast_bytes_received.sum();}


    public MsgStats sent(Address dest, int length) {
        if(!enabled)
            return this;
        if(dest == null) // multicast
            return add(num_mcasts_sent, 1, num_mcast_bytes_sent, length);
        return add(num_ucasts_sent, 1, num_ucast_bytes_sent, length);
    }

    public MsgStats sent(Message msg) {
        return (msg == null || !enabled)? this : sent(msg.dest(), msg.getLength());
    }

    public MsgStats received(Address dest, int length) {
        if(!enabled)
            return this;
        if(dest == null)
            return add(num_mcasts_received, 1, num_mcast_bytes_received, length);
        return add(num_ucasts_received, 1, num_ucast_bytes_received, length);
    }

    public MsgStats received(Message msg) {
        if(msg == null || !enabled)
            return this;
        num_single_msgs_received.increment();
        return received(msg.dest(), msg.getLength());
    }

    public MsgStats received(MessageBatch batch) {
        if(batch == null || !enabled)
            return this;
        num_batches_received.increment();
        int num_msgs=batch.size();
        int length=batch.length();
        avg_batch_size.add(num_msgs);
        if(batch.dest() == null)
            return add(num_mcasts_received, num_msgs, num_mcast_bytes_received, length);
        return add(num_ucasts_received, num_msgs, num_ucast_bytes_received, length);
    }


    public MsgStats reset() {
        Stream.of(num_ucasts_sent, num_ucasts_received,
                  num_mcasts_sent, num_mcasts_received,
                  num_single_msgs_sent, num_single_msgs_received,
                  num_batches_sent, num_batches_received,
                  num_mcast_bytes_sent, num_mcast_bytes_received,
                  num_ucast_bytes_sent, num_ucast_bytes_received)
          .forEach(LongAdder::reset);
        avg_batch_size.clear();
        return this;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean details) {
        StringBuilder sb=new StringBuilder();
        if(!details) {
            sb.append(String.format("%,d sent (%s) %,d received (%s)", getNumMsgsSent(), Util.printBytes(getNumBytesSent()),
                                    getNumMsgsReceived(), Util.printBytes(getNumBytesReceived())));
            return sb.toString();
        }
        Field[] fields=MsgStats.class.getDeclaredFields();
        for(Field field: fields) {
            try {
                Object val=Util.getField(field, this);
                sb.append(field.getName()).append(": ").append(val).append("\n");
            }
            catch(Throwable t) {

            }
        }
        return sb.toString();
    }

    protected MsgStats add(LongAdder msgs, int num_msgs, LongAdder bytes, int length) {
        msgs.add(num_msgs);
        if(length > 0)
            bytes.add(length);
        return this;
    }
}
