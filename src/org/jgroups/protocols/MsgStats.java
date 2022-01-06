package org.jgroups.protocols;

import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Class which has all the stats about received/sent messages etc (in TP)
 * @author Bela Ban
 * @since  4.0
 */
public class MsgStats {
    protected final AtomicLong     num_msgs_sent=new AtomicLong();
    protected final AtomicLong     num_msgs_received=new AtomicLong();

    protected final AtomicLong     num_ucasts_sent=new AtomicLong();
    protected final AtomicLong     num_mcasts_sent=new AtomicLong();

    protected final AtomicLong     num_ucasts_received=new AtomicLong();
    protected final AtomicLong     num_mcasts_received=new AtomicLong();

    protected final AtomicLong     num_bytes_sent=new AtomicLong();
    protected final AtomicLong     num_bytes_received=new AtomicLong();

    protected final AtomicLong     num_ucast_bytes_sent=new AtomicLong();
    protected final AtomicLong     num_mcast_bytes_sent=new AtomicLong();

    protected final AtomicLong     num_ucast_bytes_received=new AtomicLong();
    protected final AtomicLong     num_mcast_bytes_received=new AtomicLong();

    protected final AtomicLong     num_oob_msgs_received=new AtomicLong();

    protected final AtomicLong     num_single_msgs_sent=new AtomicLong();
    protected final AtomicLong     num_batches_sent=new AtomicLong();
    protected final AtomicLong     num_batches_received=new AtomicLong();

    protected final AtomicInteger num_rejected_msgs=new AtomicInteger(0);


    @ManagedAttribute(description="Number of messages sent",type=AttributeType.SCALAR)
    public long     getNumMsgsSent()                   {return num_msgs_sent.get();}
    public MsgStats incrNumMsgsSent(int d)             {num_msgs_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of unicast messages sent",type=AttributeType.SCALAR)
    public long     getNumUcastMsgsSent()              {return num_ucasts_sent.get();}
    public MsgStats incrNumUcastMsgsSent(int d)        {num_ucasts_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of multicast messages sent",type=AttributeType.SCALAR)
    public long     getNumMcastMsgsSent()              {return num_mcasts_sent.get();}
    public MsgStats incrNumMcastMsgsSent(int d)        {num_mcasts_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of unicast messages received",type=AttributeType.SCALAR)
    public long     getNumUcastMsgsReceived()          {return num_ucasts_received.get();}
    public MsgStats incrNumUcastMsgsReceived(int d)    {num_ucasts_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of multicast messages received",type=AttributeType.SCALAR)
    public long     getNumMcastMsgsReceived()          {return num_mcasts_received.get();}
    public MsgStats incrNumMcastMsgsReceived(int d)    {num_mcasts_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of regular messages received",type=AttributeType.SCALAR)
    public long     getNumMsgsReceived()               {return num_msgs_received.get();}
    public MsgStats incrNumMsgsReceived(int d)         {num_msgs_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of OOB messages received. This value is included in num_msgs_received."
      ,type=AttributeType.SCALAR)
    public long     getNumOOBMsgsReceived()            {return num_oob_msgs_received.get();}
    public MsgStats incrNumOOBMsgsReceived(int d)      {num_oob_msgs_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of single messages sent",type=AttributeType.SCALAR)
    public long     getNumSingleMsgsSent()             {return num_single_msgs_sent.get();}
    public MsgStats incrNumSingleMsgsSent(int d)       {num_single_msgs_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of message batches sent",type=AttributeType.SCALAR)
    public long     getNumBatchesSent()                {return num_batches_sent.get();}
    public MsgStats incrNumBatchesSent(int d)          {num_batches_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of message batches received",type=AttributeType.SCALAR)
    public long     getNumBatchesReceived()            {return num_batches_received.get();}
    public MsgStats incrNumBatchesReceived(int d)      {num_batches_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of bytes sent",type=AttributeType.BYTES)
    public long     getNumBytesSent()                  {return num_bytes_sent.get();}
    public MsgStats incrNumBytesSent(int d)            {num_bytes_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of unicast bytes sent",type=AttributeType.BYTES)
    public long     getNumUcastBytesSent()             {return num_ucast_bytes_sent.get();}
    public MsgStats incrNumUcastBytesSent(int d)       {num_ucast_bytes_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of multicast bytes sent",type=AttributeType.BYTES)
    public long     getNumMcastBytesSent()             {return num_mcast_bytes_sent.get();}
    public MsgStats incrNumMcastBytesSent(int d)       {num_mcast_bytes_sent.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of bytes received",type=AttributeType.BYTES)
    public long     getNumBytesReceived()              {return num_bytes_received.get();}
    public MsgStats incrNumBytesReceived(int d)        {num_bytes_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of unicast bytes received",type=AttributeType.BYTES)
    public long     getNumUcastBytesReceived()         {return num_ucast_bytes_received.get();}
    public MsgStats incrNumUcastBytesReceived(int d)   {num_ucast_bytes_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of multicast bytes received",type=AttributeType.BYTES)
    public long     getNumMcastBytesReceived()         {return num_mcast_bytes_received.get();}
    public MsgStats incrNumMcastBytesReceived(int d)   {num_mcast_bytes_received.addAndGet(d); return this;}

    @ManagedAttribute(description="Number of dropped messages that were rejected by the thread pool"
      ,type=AttributeType.SCALAR)
    public int      getNumRejectedMsgs()               {return num_rejected_msgs.get();}
    public MsgStats incrNumRejectedMsgs(int d)         {num_rejected_msgs.addAndGet(d); return this;}


    public MsgStats reset() {
        Stream.of(num_msgs_sent, num_msgs_received, num_single_msgs_sent, num_oob_msgs_received,
                  num_batches_sent, num_batches_received, num_bytes_sent,num_bytes_received)
          .forEach(al -> al.set(0));
        Stream.of(num_rejected_msgs).forEach(ai -> ai.set(0));
        return this;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
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
}
