package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.protocols.CENTRAL_LOCK2;
import org.jgroups.protocols.Locking;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

/** Tests https://issues.jboss.org/browse/JGRP-2234 with {@link LockService}
 * @author Bela Ban
 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},singleThreaded=true,dataProvider="createLockingProtocol")
public class LockService_JGRP_2234_Test {
    protected JChannel            a, b, c, d;
    protected LockService         s1, s2, s3, s4;
    protected static final String LOCK="sample-lock";
    protected static final String CLUSTER=LockService_JGRP_2234_Test.class.getSimpleName();


    @DataProvider(name="createLockingProtocol")
    Object[][] createLockingProtocol() {
        return new Object[][] {
          {CENTRAL_LOCK.class},
          {CENTRAL_LOCK2.class}
        };
    }


    protected void init(Class<? extends Locking> locking_class) throws Exception {
        a=createChannel("A", locking_class);
        s1=new LockService(a);
        a.connect(CLUSTER);

        b=createChannel("B", locking_class);
        s2=new LockService(b);
        b.connect(CLUSTER);

        c=createChannel("C", locking_class);
        s3=new LockService(c);
        c.connect(CLUSTER);

        d=createChannel("D", locking_class);
        s4=new LockService(d);
        d.connect(CLUSTER);

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c, d);
    }


    @AfterMethod
    protected void cleanup() {
        Util.close(d, c, b, a);
    }

    @BeforeMethod
    protected void unlockAll() {
        Stream.of(s4,s3,s2,s1).forEach(s -> {
            if(s != null) s.unlockAll();
        });
        Thread.interrupted(); // clears any possible interrupts from the previous method
    }

    /**
     * The initial view is {A,B,C,D}. D holds the lock and unlocks it (on A), but the view is already {B,C,D} as A has
     * left. However, at the time of the unlock request, the view is still {A,B,C,D} on D so the request is sent to A.<br/>
     * The unlock request from D (to the new coord B) is therefore lost and the lock is never released.<br/>
     * Therefore, when C tries to acquire the lock, it will fail as B thinks the lock is still held by D.<br/>
     * The lost request (due to the new view not being received at all members at the same wall-clock time) is simulated
     * by a simple dropping of the release request on D.
     */
    public void testUnsuccessfulUnlock(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        Lock lock=s4.getLock(LOCK);
        boolean success=lock.tryLock(10, TimeUnit.SECONDS); // this should succeed as A is the lock server for LOCK
        assert success;

        d.getProtocolStack().insertProtocol(new UnlockDropper(), ProtocolStack.Position.BELOW, Locking.class);
        lock.unlock(); // this request will be dropped

        d.getProtocolStack().removeProtocol(UnlockDropper.class); // future release requests are not going to be dropped


        a.close(); // B will be the new coordinator

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, b,c,d);


        Lock lock2=s3.getLock(LOCK); // C tries to acquire the lock
        success=lock2.tryLock(5, TimeUnit.SECONDS);
        assert success;
    }


    protected static JChannel createChannel(String name, Class<? extends Locking> locking_class) throws Exception {
        Protocol[] stack=Util.getTestStack(locking_class.getDeclaredConstructor().newInstance().level("trace"));
        return new JChannel(stack).name(name);
    }


    protected static void lock(Lock lock, String name) {
        System.out.println("[" + Thread.currentThread().getId() + "] locking " + name);
        lock.lock();
        System.out.println("[" + Thread.currentThread().getId() + "] locked " + name);
    }

    protected static boolean tryLock(Lock lock, String name) {
        System.out.println("[" + Thread.currentThread().getId() + "] tryLocking " + name);
        boolean rc=lock.tryLock();
        System.out.println("[" + Thread.currentThread().getId() + "] " + (rc? "locked " : "failed locking ") + name);
        return rc;
    }

    protected static boolean tryLock(Lock lock, long timeout, String name) throws InterruptedException {
        System.out.println("[" + Thread.currentThread().getId() + "] tryLocking " + name);
        boolean rc=lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        System.out.println("[" + Thread.currentThread().getId() + "] " + (rc? "locked " : "failed locking ") + name);
        return rc;
    }

    protected static void unlock(Lock lock, String name) {
        if(lock == null)
            return;
        System.out.println("[" + Thread.currentThread().getId() + "] releasing " + name);
        lock.unlock();
        System.out.println("[" + Thread.currentThread().getId() + "] released " + name);
    }
    

    protected static class UnlockDropper extends Protocol {

        public Object down(Message msg) {
            Locking lock_prot=(Locking)up_prot;
            short CENTRAL_LOCK_ID=ClassConfigurator.getProtocolId(lock_prot.getClass());
            Locking.LockingHeader hdr=msg.getHeader(CENTRAL_LOCK_ID);
            if(hdr != null) {
                try {
                    Locking.Request req=Util.streamableFromBuffer(Locking.Request::new, msg.getArray(), msg.getOffset(), msg.getLength());
                    switch(req.getType()) {
                        case RELEASE_LOCK:
                            System.out.printf("%s ---- dropping %s\n", up_prot.getProtocolStack().getChannel().getAddress(), req);
                            return null;
                    }
                }
                catch(Exception ex) {
                    log.error("failed deserializing request", ex);
                    return null;
                }
            }
            return down_prot.down(msg);
        }
    }

}
