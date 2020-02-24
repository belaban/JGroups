package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Message;
import org.jgroups.util.NonBlockingCreditMap;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.util.NonBlockingCreditMap}
 * @author Bela Ban
 * @since  4.0.4
 */
@Test(singleThreaded=true)
public class NonBlockingCreditMapTest {
    protected static final int MAX_CREDITS=10000;
    protected static final int MAX_QUEUE_SIZE=60_000; // bytes
    protected static Address a=Util.createRandomAddress("A");
    protected static Address b=Util.createRandomAddress("B");
    protected static Address c=Util.createRandomAddress("C");
    protected static Address d=Util.createRandomAddress("D");

    protected NonBlockingCreditMap map;


    @BeforeMethod protected void setup() {
        map=new NonBlockingCreditMap(MAX_CREDITS, MAX_QUEUE_SIZE, new ReentrantLock(true));
        addAll();
    }

    public void testDecrement() {
        Message msg=new BytesMessage(null, new byte[8000]);
        boolean rc=map.decrement(msg, msg.getLength(), 0); // timeout will be ignored
        assert rc && map.getMinCredits() == 2000;
        assert !map.isQueuing();
        msg=new BytesMessage(null, new byte[2000]);
        rc=map.decrement(msg, msg.getLength(), 0);
        assert rc && !map.isQueuing();

        for(int i=0; i < 5; i++) {
            msg=new BytesMessage(null, new byte[1000]);
            rc=map.decrement(msg, msg.getLength(), 0);
            assert !rc && map.isQueuing();
        }
        assert map.getQueuedMessages() == 5 && map.getQueuedMessageSize() == 5000;

        map.replenish(d, 4500);
        assert map.getMinCredits() == 0;
        assert map.isQueuing();
        for(Address member: Arrays.asList(a,b,c))
            map.replenish(member, 4500);
        assert map.isQueuing() && map.getQueuedMessages() == 1;
    }

    public void testBlockingDecrement() {
        map=new NonBlockingCreditMap(MAX_CREDITS, 2500, new ReentrantLock(true));
        addAll();

        Message msg=new BytesMessage(null, new byte[8000]);
        boolean rc=map.decrement(msg, msg.getLength(), 0);
        assert rc && !map.isQueuing();

        new Thread(() -> {
            Util.sleep(2000);
            System.out.println("\n-- replenishing 5000 credits");
            Stream.of(a, b, c, d).forEach(c -> map.replenish(c, 5000));
        }).start();

        for(int i=1; i <= 5; i++) {
            msg=new BytesMessage(null, new byte[1000]);
            System.out.printf("-- adding msg %d: ", i);
            rc=map.decrement(msg, msg.getLength(), 0); // message 5 should block, but replenish() should unblock it
            System.out.printf("rc=%b\n", rc);
            assert rc == i < 3;
        }
        assert !map.isQueuing();
        assert map.getQueuedMessages() == 0;
    }


    /** Multiple threads block on credits from B, then the CreditMap is cleared. All threads should be unblocked */
    public void testDecrementAndClear() throws TimeoutException {
        addAll();
        boolean rc=map.decrement(null, 10000, 100);
        assert rc;
        System.out.println("map = " + map);

        for(Address addr: Arrays.asList(a,c,d))
            map.replenish(addr, 1000);
        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(()-> map.decrement(msg(65000), 65000, 60000));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        map.clear();
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
    }

    /** Multiple threads block on credits from B, then the CreditMap is cleared. All threads should be unblocked */
    public void testDecrementAndReset() throws TimeoutException {
        addAll();
        boolean rc=map.decrement(null, 10000, 100);
        assert rc;
        System.out.println("map = " + map);

        for(Address addr: Arrays.asList(a,c,d))
            map.replenish(addr, 1000);
        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(()-> map.decrement(msg(65000), 65000, 60000));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        map.reset();
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
        rc=map.decrement(null, 1000, 500);
        assert !rc;
    }

    protected void addAll() {
        map.putIfAbsent(a); map.putIfAbsent(b); map.putIfAbsent(c); map.putIfAbsent(d);
    }

    protected static Message msg(int len) {
        return new BytesMessage(null, new byte[len]);
    }

}
