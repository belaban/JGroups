package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Message;
import org.jgroups.util.NonBlockingCreditMap;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
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
            System.out.printf("\n-- replenishing 5000 credits\n");
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


    protected void addAll() {
        map.putIfAbsent(a); map.putIfAbsent(b); map.putIfAbsent(c); map.putIfAbsent(d);
    }


}
