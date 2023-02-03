package org.jgroups.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.jgroups.util.Util.printTime;

/**
 * Maintains averages for time values measured between {@link #start} and {@link #stop}.
 * @author Bela Ban
 * @since  5.2.13
 */
public class Profiler {
    protected final AverageMinMax avg=new AverageMinMax().unit(NANOSECONDS);
    protected long                start;

    public Profiler() {
    }

    public void reset() {avg.clear();}

    public void start() {
        start=System.nanoTime();
    }

    public void stop() {

        if(start > 0) {
            long time=System.nanoTime() - start;
            avg.add(time);
            start=0;
        }
    }

    @Override
    public String toString() {
        return String.format("min/avg/max=%s/%s/%s", printTime(avg.min(), NANOSECONDS),
                             printTime(avg.average(), NANOSECONDS), printTime(avg.max(), NANOSECONDS));
    }
}
