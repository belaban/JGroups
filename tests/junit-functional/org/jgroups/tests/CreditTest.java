package org.jgroups.tests;

import org.jgroups.util.Credit;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Tests {@link org.jgroups.util.Credit}
 * @author Bela Ban
 * @since  4.0.4
 */
@Test
public class CreditTest {
    protected static final int MIN_CREDITS=2000, MAX_CREDITS=10000;

    public void testGet() {
        Credit cred=new Credit(MAX_CREDITS);
        System.out.println("cred = " + cred);
        assert cred.get() == MAX_CREDITS;
    }

    public void testConditionalDecrement() {
        Credit cred=new Credit(MAX_CREDITS);
        boolean success=cred.decrementIfEnoughCredits(null, 8000, 500);
        assert success && cred.get() == 2000;

        success=cred.decrementIfEnoughCredits(null, 2000, 500);
        assert success && cred.get() == 0;

        success=cred.decrementIfEnoughCredits(null, 1000, 500);
        assert !success && cred.get() == 0;
    }


    public void testDecrementAndGet() {
        Credit cred=new Credit(MAX_CREDITS);
        long retval=cred.decrementAndGet(2000, MIN_CREDITS, MAX_CREDITS);
        assert retval == 0 && cred.get() == 8000;

        retval=cred.decrementAndGet(6000, MIN_CREDITS, MAX_CREDITS);
        assert retval == 8000 && cred.get() == MAX_CREDITS;

        retval=cred.decrementAndGet(9000, MIN_CREDITS, MAX_CREDITS);
        assert retval == 9000 && cred.get() == MAX_CREDITS;

        retval=cred.decrementAndGet(MAX_CREDITS, MIN_CREDITS, MAX_CREDITS);
        assert retval == MAX_CREDITS && cred.get() == MAX_CREDITS;

        retval=cred.decrementAndGet(MAX_CREDITS+1000, MIN_CREDITS, MAX_CREDITS);
        assert retval == MAX_CREDITS && cred.get() == MAX_CREDITS;
    }

    public void testIncrement() {
        Credit cred=new Credit(MAX_CREDITS);
        long retval=cred.decrementAndGet(5000, MIN_CREDITS, MAX_CREDITS);
        assert retval == 0 && cred.get() == 5000;

        cred.increment(1000, MAX_CREDITS);
        assert cred.get() == 6000;

        cred.increment(9000, MAX_CREDITS);
        assert cred.get() == MAX_CREDITS;
    }

    public void testIncrementOnMultipleDecrementers() {
        Credit cred=new Credit(0);
        Thread[] decrementers=new Thread[20];
        for(int i=0; i < decrementers.length; i++) {
            decrementers[i]=new Thread(() -> cred.decrementIfEnoughCredits(null, 1000, 30000));
            decrementers[i].start(); // every thread blocks until credits are available
        }
        Util.sleep(1000);
        cred.increment(20000, 30000);
        Util.sleep(1000);
        assert cred.get() == 0;
        for(Thread decr: decrementers)
            assert !decr.isAlive();
    }

    public void testIncrementOnMultipleDecrementersAndReset() throws TimeoutException {
        Credit cred=new Credit(0);
        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(() -> cred.decrementIfEnoughCredits(null, 1000, 20000));
            threads[i].start(); // every thread blocks until credits are available
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TIMED_WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
        assert cred.get() == 0;

        cred.reset();
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        assert cred.get() == 0;
        for(Thread decr: threads)
            assert !decr.isAlive();
    }

    public void testNeedToSendCredits() {
        Credit cred=new Credit(MAX_CREDITS);
        boolean send=cred.needToSendCreditRequest(10);
        assert send;
        send=cred.needToSendCreditRequest(10);
        assert !send;
        Util.sleep(100);
        send=cred.needToSendCreditRequest(10);
        assert send;
    }
}
