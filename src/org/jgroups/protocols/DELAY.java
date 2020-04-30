package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


/**
 * Delays incoming/outgoing messages by a random number of milliseconds (range between 0 and n
 * where n is determined by the user) and nanoseconds (constant amount).
 * <p>
 * Incoming messages can be delayed independently from outgoing messages (or not delayed at all).
 * <p>
 * This protocol should be inserted directly above the transport protocol (e.g. UDP).
 *
 * @author Bela Ban
 * @author Sanne Grinovero
 * @author Matej Cimbora
 */
@MBean(description="Written by Sanne")
public class DELAY extends Protocol {

    private static final Random randomNumberGenerator = new Random();

    @Property(description="Upper bound of number of milliseconds to delay passing a message up the stack (exclusive)",
      type=AttributeType.TIME)
    protected int in_delay;
    @Property(description = "Upper bound number of milliseconds to delay passing a message down the stack (exclusive)",
    type=AttributeType.TIME)
    protected int out_delay;
    @Property(description = "Number of nanoseconds to delay passing a message up the stack",
      type=AttributeType.TIME,unit=TimeUnit.NANOSECONDS)
    protected int in_delay_nanos;
    @Property(description = "Number of nanoseconds to delay passing a message down the stack",
      type=AttributeType.TIME,unit=TimeUnit.NANOSECONDS)
    protected int out_delay_nanos;
    @Property(description = "Keep the delay constant. By default delay time randoms between 0 and upper bound")
    protected boolean constant_delay;

    protected DelayedMessageHandler delayed_message_handler;
    protected DelayQueue<DelayedMessage> delayed_messages = new DelayQueue<>();

    public int  getInDelay()               {return in_delay;}
    public void setInDelay(int in_delay)   {this.in_delay=in_delay;}
    public int  getOutDelay()              {return out_delay;}
    public void setOutDelay(int out_delay) {this.out_delay=out_delay;}
    public int  getInDelayNanos()               {return in_delay_nanos;}
    public void setInDelayNanos(int in_delay_nanos)   {this.in_delay_nanos=in_delay_nanos;}
    public int  getOutDelayNanos()              {return out_delay_nanos;}
    public void setOutDelayNanos(int out_delay_nanos) {this.out_delay_nanos=out_delay_nanos;}

    @Override
    public void init() throws Exception {
        super.init();
        delayed_message_handler = new DelayedMessageHandler();
        delayed_message_handler.start();
    }

    @Override
    public void destroy() {
        super.destroy();
        if (delayed_message_handler != null)
            Util.interruptAndWaitToDie(delayed_message_handler);
    }

    public Object down(Message msg) {
        delayed_messages.add(new DelayedMessage(msg, System.nanoTime()));
        return null;
    }

    public Object up(Message msg) {
        sleep(in_delay, in_delay_nanos);
        return up_prot.up(msg);
    }

    public void up(final MessageBatch batch) {
        sleep(in_delay, in_delay_nanos);
        up_prot.up(batch);
    }

    /**
     * Compute a random number between 0 and n
     */
    private int computeDelay(final int n) {
        if (n <= 1) {
            return 0;
        }
        return constant_delay ? n : randomNumberGenerator.nextInt(n);
    }

    private void sleep(final int variable_milliseconds_delay, final int nano_delay) {
        final int millis = computeDelay(variable_milliseconds_delay);
        if (millis != 0 || nano_delay != 0) {
            Util.sleep(millis, nano_delay);
        }
    }

    private class DelayedMessage implements Delayed {
        private final Message msg;
        private final long    start;
        private final long    delay_time;

        public DelayedMessage(Message msg, long start) {
            this.msg = msg;
            this.start = start;
            this.delay_time = TimeUnit.NANOSECONDS.convert(computeDelay(out_delay), TimeUnit.MILLISECONDS) + out_delay_nanos;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(start + delay_time - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o == this)
                return 0;
            return Long.compare(this.getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
        }
    }

    private class DelayedMessageHandler extends Thread {

        private final List<DelayedMessage> buffer = new ArrayList<>();

        @Override
        public void run() {
            for (;;) {
                try {
                    delayed_messages.drainTo(buffer);
                    for (DelayedMessage del_msg : buffer)
                        down_prot.down(del_msg.msg);
                    buffer.clear();
                } catch (Exception e) {
                    // handling thread should never die
                }
            }
        }
    }

}
