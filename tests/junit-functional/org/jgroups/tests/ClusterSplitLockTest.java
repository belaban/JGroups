package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/** Tests https://issues.jboss.org/browse/JGRP-2234 */
@Test(groups = {Global.FUNCTIONAL, Global.EAP_EXCLUDED}, timeOut = 60000)
public class ClusterSplitLockTest {

    private static final int MEMBERS = 3;
    private final JChannel[] channels = new JChannel[MEMBERS];
    private final LockService[] lockServices = new LockService[MEMBERS];
    private final ExecutorService[] execs = new ExecutorService[MEMBERS];

    @BeforeMethod
    protected void setUp() throws Exception {
        for (int i = 0; i < MEMBERS; i++) {

            Protocol[] stack = Util.getTestStack(new CENTRAL_LOCK()
                    .level("debug")
                    .setValue("num_backups", 2));

            channels[i] = new JChannel(stack);

            lockServices[i] = new LockService(channels[i]);
            channels[i].setName(memberName(i));
            channels[i].connect("TEST");
            execs[i] = Executors.newCachedThreadPool();

            if (i == 0) {
                Util.sleep(500);
            }
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);

        // Make sure A is coordinator, because we blindly assume it is in the tests below.
        assertTrue(channels[0].getView().getCoord().equals(channels[0].getAddress()));
    }

    private void disconnectAndDestroy(int i) throws Exception {
        JChannel channel = channels[i];
        channel.disconnect();
        channel.getProtocolStack().destroy();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        for (int i = MEMBERS - 1; i >= 0; i--) {
            disconnectAndDestroy(i);
            execs[i].shutdown();
            assertTrue(execs[i].awaitTermination(5, SECONDS));
        }
    }

    /**
     * Performs a test where the first member (also the initial coordinator)
     * goes down
     *
     * @throws Exception
     */
    public void testClusterSplitWhereAGoesDown() throws Exception {
        testClusterSplitImpl(0);
    }

    /**
     * Performs a test where the second member goes down
     *
     * @throws Exception
     */
    public void testClusterSplitWhereBGoesDown() throws Exception {
        testClusterSplitImpl(1);
    }

    /**
     * Performs a test where the third member goes down
     *
     * @throws Exception
     */
    public void testClusterSplitWhereCGoesDown() throws Exception {
        testClusterSplitImpl(2);
    }

    /**
     * Performs a test where the specified downMember goes down when the member
     * performed a lock operation on 50% of the locks. The coordinator should
     * unlock all locks that where locked by the member that goes down. If the
     * member that goes down is the coordinator the new channel coordinator
     * should make sure the lock table is up-to-date.
     *
     * @param downMember
     * @throws Exception
     */
    private void testClusterSplitImpl(final int downMember) throws Exception {
        CountDownLatch doneOnMemberThatWillGoDown = new CountDownLatch(1);

        final int numLocks = 10;
        final int halfway = numLocks / 2;

        List<Future<?>> futures = new ArrayList<>();

        /*
         * All members perform the specified number of lock() operations. The
         * 'downMember' disconnects half way
         */
        for (int i = 0; i < MEMBERS; i++) {
            final int mbrIdx = i;

            final AtomicInteger unlockCount = new AtomicInteger(0);

            for (int j = 0; j < numLocks; j++) {
                final int lockNr = j;

                if (mbrIdx == downMember) {
                    if (lockNr == halfway) {
                        futures.add(execs[downMember].submit(() -> {
                            try {
                                doneOnMemberThatWillGoDown.await();
                                log("Disconnecting member %s", memberName(downMember));
                                disconnectAndDestroy(downMember);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    }
                    if (lockNr >= halfway) {
                        break;
                    }
                }

                futures.add(execs[mbrIdx].submit(() -> {

                    Lock lock = lockServices[mbrIdx].getLock("testlock" + lockNr);

                    try {
                        if (!lock.tryLock(5, SECONDS)) {
                            if (mbrIdx == downMember) {
                                fail(String.format("Member %s failed to lock %s using tryLock in healthy situation.", memberName(mbrIdx), lockNr));
                            } else {
                                log("Failed to tryLock member:%s lock:%d LOCKS:\n%s", memberName(mbrIdx), lockNr, lockServices[mbrIdx].printLocks());
                                return;
                            }
                        } else {
                            log("Member %s locked %d (threadid: %d)", memberName(mbrIdx), lockNr, Thread.currentThread().getId());
                        }
                    } catch (InterruptedException ie) {
                        log("InterruptedException member:%s, lock:%d", memberName(mbrIdx), lockNr, ie);
                        fail("Interrupted on tryLock " + memberName(mbrIdx) + " - " + lockNr);
                    }

                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        fail("Interrupted while sleeping.");
                    } finally {
                        lock.unlock();
                        log("Unlocked lock %d by member %s (threadid: %d)", lockNr, memberName(mbrIdx), Thread.currentThread().getId());

                        if (mbrIdx == downMember && halfway == unlockCount.incrementAndGet()) {
                            log("setting doneOnMemberThatWillGoDown flag");
                            doneOnMemberThatWillGoDown.countDown();
                        }
                    }
                }));
            }

        }

        /* wait for the chaos to disappear */
        for (Future<?> fut : futures) {
            fut.get();
        }

        StringBuilder locksOverview = new StringBuilder("\n==== first run done ====\n");
        for (int i = 0; i < MEMBERS; i++) {
            locksOverview.append(String.format("Locks on member %s:\n%s\n", memberName(i), lockServices[i].printLocks()));
        }
        locksOverview.append("\n========================");
        log(locksOverview.toString());

        /*
         * All locks should be unlocked at this point so no try lock request
         * should fail
         */
        Thread.sleep(2000);
        log("==== Checking if tryLock succeeds for all locks on all remaining members =====");
        for (int i = 0; i < MEMBERS; i++) {
            if (i == downMember) {
                continue;
            }

            for (int j = 0; j < numLocks; j++) {
                Lock l = lockServices[i].getLock("testlock" + j);
                if (!l.tryLock()) {
                    logError("Failed to acquire lock on %d by member %s", j, memberName(i));
                    Address coord = channels[i].getView().getCoord();
                    int count = 0;
                    for (JChannel c : channels) {
                        if (null != c.getAddress() && c.getAddress().equals(coord)) {
                            logError("Lock table for %s (coord):\n%s", coord, lockServices[count].printLocks());
                            break;
                        }
                        count++;
                    }
                    fail(String.format("Member %s can't lock:%d", memberName(i), j));
                }
                l.unlock();
            }

        }

    }

    private String memberName(int mbrIndex) {
        return String.valueOf((char) ('A' + mbrIndex));
    }

    private void log(String fmt, Object... args) {
        log(System.out, fmt, args);
    }

    private void logError(String fmt, Object... args) {
        log(System.err, fmt, args);
    }

    private void log(PrintStream out, String fmt, Object... args) {
        out.println(String.format(fmt, args));
    }
}
