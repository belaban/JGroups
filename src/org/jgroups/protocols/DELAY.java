package org.jgroups.protocols;

import java.util.Random;

import org.jgroups.Event;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;


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
 */
public class DELAY extends Protocol {

    private static final Random randomNumberGenerator = new Random();

    @Property(description = "Upper bound of number of milliseconds to delay passing a message up the stack (exclusive)")
    protected int in_delay;
    @Property(description = "Upper bound number of milliseconds to delay passing a message down the stack (exclusive)")
    protected int out_delay;
    @Property(description = "Number of nanoseconds to delay passing a message up the stack")
    protected int in_delay_nanos;
    @Property(description = "Number of nanoseconds to delay passing a message down the stack")
    protected int out_delay_nanos;

    public int  getInDelay()               {return in_delay;}
    public void setInDelay(int in_delay)   {this.in_delay=in_delay;}
    public int  getOutDelay()              {return out_delay;}
    public void setOutDelay(int out_delay) {this.out_delay=out_delay;}
    public int  getInDelayNanos()               {return in_delay_nanos;}
    public void setInDelayNanos(int in_delay_nanos)   {this.in_delay_nanos=in_delay_nanos;}
    public int  getOutDelayNanos()              {return out_delay_nanos;}
    public void setOutDelayNanos(int out_delay_nanos) {this.out_delay_nanos=out_delay_nanos;}

    public Object down(final Event evt) {
        if (isMessage(evt))
            sleep(out_delay, out_delay_nanos);
        return down_prot.down(evt);
    }

    public Object up(final Event evt) {
        if (isMessage(evt))
            sleep(in_delay, in_delay_nanos);
        return up_prot.up(evt);
    }

    public void up(final MessageBatch batch) {
        sleep(in_delay, in_delay_nanos);
        up_prot.up(batch);
    }

    private static boolean isMessage(final Event evt) {
        return evt.getType() == Event.MSG;
    }

    /**
     * Compute a random number between 0 and n
     */
    private static int computeDelay(final int n) {
        if (n <= 1) {
            return 0;
        }
        return randomNumberGenerator.nextInt(n);
    }

    private static void sleep(final int variable_milliseconds_delay, final int nano_delay) {
        final int millis = computeDelay(variable_milliseconds_delay);
        if (millis != 0 || nano_delay != 0) {
            Util.sleep(millis, nano_delay);
        }
    }

}

