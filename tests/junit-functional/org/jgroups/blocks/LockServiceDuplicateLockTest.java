package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.blocks.locking.LockNotification;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.CENTRAL_LOCK2;
import org.jgroups.protocols.Locking;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Owner;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests the lock service under partitions: when members in different partitions hold the same lock, after a merge, all
 * but one lock holder will get notified that their locks have been revoked.
 * See https://issues.jboss.org/browse/JGRP-2249 for details.
 * @author Bela Ban
 * @since  4.0.13
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class LockServiceDuplicateLockTest implements LockNotification {
    protected final JChannel[]     channels=new JChannel[6];
    protected final LockService[]  lock_services=new LockService[channels.length];
    protected Lock                 lock_3; // lock held by member 3
    protected Lock                 lock_6; // lock held by member 6
    protected static final String  LOCK_NAME="X";

    @BeforeMethod protected void setup() throws Exception {
        for(int i=0; i < channels.length; i++) {
            channels[i]=create(i + 1).connect(LockServiceDuplicateLockTest.class.getSimpleName());
            lock_services[i]=new LockService(channels[i]);
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
        System.out.printf("channels:\n%s", Stream.of(channels)
          .map(ch -> String.format("%s: %s\n", ch.getAddress(), ch.getView()))
          .collect(Collectors.joining("\n")));
        lock_3=lock_services[2].getLock(LOCK_NAME);
        lock_6=lock_services[5].getLock(LOCK_NAME);
        for(JChannel ch: channels) {
            Locking l=ch.getProtocolStack().findProtocol(Locking.class);
            l.addLockListener(this);
        }
    }

    @AfterMethod protected void destroy() {
        trace(false, channels);
        Util.closeReverse(channels);
    }


    public void testDuplicateLockRevocation() throws Exception {
        boolean lock_acquired=lock_3.tryLock(3, TimeUnit.SECONDS);
        System.out.printf("** lock_3: %s\n", lock_3);
        assert lock_acquired;
        System.out.println("--------- Injecting partitions ---------");

        createAndInjectView(channels[0], channels[1], channels[2]);
        createAndInjectView(channels[3], channels[4], channels[5]);
        System.out.printf("channels:\n%s", Stream.of(channels)
          .map(ch -> String.format("%s: %s\n", ch.getAddress(), ch.getView()))
          .collect(Collectors.joining("\n")));
        Stream.of(channels[0], channels[1], channels[2]).allMatch(ch -> ch.getView().size() == 3);
        Stream.of(channels[3], channels[4], channels[5]).allMatch(ch -> ch.getView().size() == 3);

        // now acquire the same lock on member 6: this will succeed because we have 2 partitions
        lock_acquired=lock_6.tryLock(1, TimeUnit.SECONDS);
        System.out.printf("** lock_6: %s\n", lock_6);
        assert lock_acquired;

        System.out.println("----------- Merging partitions ----------");
        trace(true, channels);
        MergeView mv=createMergeView(channels);
        injectView(mv, channels);

        System.out.printf("channels:\n%s", Stream.of(channels)
          .map(ch -> String.format("%s: %s\n", ch.getAddress(), ch.getView()))
          .collect(Collectors.joining("\n")));
        Stream.of(channels).allMatch(ch -> ch.getView().size() == channels.length);

        System.out.printf("lock_3: %s, lock_6: %s\n", lock_3, lock_6);

        printLockTables(channels);

        assertServerLocks(1,  0);
        assertServerLocks(0,  1,2,3,4,5);

        assertClientLocks(1,  2);
        assertClientLocks(0,  0,1,3,4,5);
    }


    protected void assertServerLocks(int num, int ... indices) {
        for(int index: indices) {
            JChannel ch=channels[index];
            Locking lock=ch.getProtocolStack().findProtocol(Locking.class);
            assert lock.getNumServerLocks() == num
              : String.format("expected %d server locks but found %d in %s", num, lock.getNumServerLocks(), ch.getAddress());
        }
    }

    protected void assertClientLocks(int num, int ... indices) {
        for(int index: indices) {
            JChannel ch=channels[index];
            Locking lock=ch.getProtocolStack().findProtocol(Locking.class);
            assert lock.getNumClientLocks() == num
              : String.format("expected %d client locks but found %d in %s", num, lock.getNumClientLocks(), ch.getAddress());
        }
    }

    @Test(enabled=false) public void lockCreated(String name) {}
    @Test(enabled=false) public void lockDeleted(String name) {}

    @Test(enabled=false) public void lockRevoked(String lock_name, Owner current_owner) {
        System.out.printf("*** received lock revocation for %s (current owner=%s); force-unlocking lock\n",
                          lock_name, current_owner);
        lock_services[5].unlockForce(lock_name);
    }

    @Test(enabled=false) public void locked(String lock_name, Owner owner) {}
    @Test(enabled=false) public void unlocked(String lock_name, Owner owner) {}
    @Test(enabled=false) public void awaiting(String lock_name, Owner owner) {}
    @Test(enabled=false) public void awaited(String lock_name, Owner owner) {}

    protected static void createAndInjectView(JChannel... channels) throws Exception {
        Address[] addrs=new Address[channels.length];
        for(int i=0; i < channels.length; i++)
            addrs[i]=channels[i].getAddress();
        View v=View.create(addrs[0], channels[0].getView().getViewId().getId()+1, addrs);
        injectView(v, channels);
    }

    protected static MergeView createMergeView(JChannel... channels) throws Exception {
        Address[] addrs=new Address[channels.length];
        for(int i=0; i < channels.length; i++)
            addrs[i]=channels[i].getAddress();
        List<View> subgroups=new ArrayList<>();
        for(JChannel ch: channels)
            subgroups.add(ch.getView());
        return new MergeView(new ViewId(addrs[0], channels[0].getView().getViewId().getId()+1), addrs, subgroups);
    }

    protected static void injectView(View v, JChannel... channels) throws Exception {
        Stream.of(channels).forEach(ch -> {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(v);
        });
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
    }

    protected static void trace(boolean on, JChannel... channels) {
        Stream.of(channels).forEach(ch -> {
            ch.getProtocolStack().findProtocol(Locking.class).level(on? "trace" : "warn");
        });
    }

    protected static void printLockTables(JChannel... channels) {
        System.out.printf("\n\nlock tables:\n%s\n",
                          Stream.of(channels).map(ch -> {
                              CENTRAL_LOCK2 l=ch.getProtocolStack().findProtocol(CENTRAL_LOCK2.class);
                              return ch.getAddress() + ": " + l.printLocks();
                          }).collect(Collectors.joining("\n")));
    }

    protected static JChannel create(int num) throws Exception {
        return new JChannel(Util.getTestStack(new CENTRAL_LOCK2())).name(String.valueOf(num))
          // the address generator makes sure that 2's UUID is lower than 3's UUID, so 2 is chosen as merge leader
          .addAddressGenerator(() -> new UUID(0, num));
    }
}
