package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Measures incoming and outgoing rates: messages/sec, bytes/sec. This is done per destination and total.
 * @author Bela Ban
 * @since  5.3.5
 */
@MBean(description="Measures incoming and outgoing message rates")
public class RATE extends Protocol {

    @Property(description="Computes the size of a message as the payload size (false) or the serialized size (true)")
    protected boolean           measure_serialized_size;

    @Property(description="Interval (ms) at which to measure the accumulated traffic")
    protected long              interval=1000;

    @Property(description="Last N measurements to keep in history")
    protected int               history_size=60;

    protected volatile long     current_send_rate, highest_send_rate;
    protected volatile long     current_receive_rate, highest_receive_rate;

    protected final LongAdder   out=new LongAdder();
    protected final LongAdder   in=new LongAdder();

    protected BoundedList<Long> send_history, recv_history;

    protected TimeScheduler     timer;
    protected Future<?>         f;

    protected final Runnable task=() -> {
        current_send_rate=out.sumThenReset();
        if(current_send_rate > 0) {
            if(current_send_rate > highest_send_rate)
                highest_send_rate=current_send_rate;
            send_history.add(current_send_rate);
        }
        current_receive_rate=in.sumThenReset();
        if(current_receive_rate > 0) {
            if(current_receive_rate > highest_receive_rate)
                highest_receive_rate=current_receive_rate;
            recv_history.add(current_receive_rate);
        }
    };

    public boolean measureSerializedSize()          {return measure_serialized_size;}
    public RATE    measureSerializedSize(boolean b) {measure_serialized_size=b; return this;}
    public long    interval()                       {return interval;}
    public RATE    interval(long i)                 {interval=i; return this;}


    @ManagedAttribute(description="Current send rate")
    public String currentSendRate() {return Util.printBytes(current_send_rate);}

    @ManagedAttribute(description="Highest send rate")
    public String highestSendRate() {return Util.printBytes(highest_send_rate);}

    @ManagedAttribute(description="Current receive rate")
    public String currentReceiveRate() {return Util.printBytes(current_receive_rate);}

    @ManagedAttribute(description="Highest receive rate")
    public String highestReceiveRate() {return Util.printBytes(highest_receive_rate);}


    @Override
    public void init() throws Exception {
        super.init();
        send_history=new BoundedList<>(history_size);
        recv_history=new BoundedList<>(history_size);
        timer=Objects.requireNonNull(getTransport().getTimer());
    }

    @Override
    public void start() throws Exception {
        super.start();
        f=timer.scheduleWithFixedDelay(task, interval, interval, TimeUnit.MILLISECONDS, false);
    }

    @Override
    public void stop() {
        super.stop();
        f.cancel(false);
    }

    @Override
    public Object down(Message msg) {
        long size=size(msg);
        out.add(size);
        return down_prot.down(msg);
    }

    @Override
    public Object up(Message msg) {
        long size=size(msg);
        in.add(size);
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        long size=size(batch);
        in.add(size);
        up_prot.up(batch);
    }


    @Override
    public void resetStats() {
        highest_send_rate=highest_receive_rate=0;
    }

    @ManagedOperation(description="Prints the send history (skipping 0 values)")
    public String printSendHistory() {
        return send_history.stream().map(Util::printBytes).collect(Collectors.joining(","));
    }

    @ManagedOperation(description="Prints the receive history (skipping 0 values)")
    public String printReceiveHistory() {
        return recv_history.stream().map(Util::printBytes).collect(Collectors.joining(","));
    }

    protected long size(Message msg) {return measure_serialized_size? msg.serializedSize() : msg.getLength();}
    protected long size(MessageBatch batch) {return measure_serialized_size? batch.totalSize() : batch.length();}


}
