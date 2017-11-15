package org.jgroups.tests;

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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Tests concurrent access to the locks provided by {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups={Global.BYTEMAN,Global.EAP_EXCLUDED},singleThreaded=true,dataProvider="createLockingProtocol")
public class LockServiceConcurrencyTest {
    protected JChannel           a, b;
    protected LockService        ls_a, ls_b;


    @DataProvider(name="createLockingProtocol")
    Object[][] createLockingProtocol() {
        return new Object[][] {
          {CENTRAL_LOCK.class},
          {CENTRAL_LOCK2.class}
        };
    }

    protected void init(Class<? extends Locking> locking_class) throws Exception {
        a=new JChannel(Util.getTestStack(locking_class.getDeclaredConstructor().newInstance())).name("A");
        ls_a=new LockService(a);
        a.connect("LockServiceConcurrencyTest");
        b=new JChannel(Util.getTestStack(locking_class.getDeclaredConstructor().newInstance())).name("B");
        ls_b=new LockService(b);
        b.connect("LockServiceConcurrencyTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
    }

    @AfterMethod protected void destroy() {
        ls_a.unlockAll();
        ls_b.unlockAll();
        Util.close(b,a);
    }

    /** Tests JIRA https://issues.jboss.org/browse/JGRP-1679 */
    // @Test(invocationCount=100,dataProvider="createLockingProtocol")
    public void testConcurrentClientLocks(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);

        Lock lock=ls_b.getLock("L"); // A is the coordinator

        DropGrantResponse dropper=new DropGrantResponse();
        a.getProtocolStack().insertProtocol(dropper, ProtocolStack.Position.BELOW, Locking.class);

        // we're dropping the LOCK-GRANTED response for lock-id #1, so this lock acquisition must fail; lock L will not be released!
        boolean success=lock.tryLock(1, TimeUnit.MILLISECONDS);
        assert !success : "the lock acquisition should have failed";


        // the LOCK-GRANTED response for lock-id #2 is received, which is incorrect and therefore dropped
        // tryLock() works the same, with or without timeout
        success=lock.tryLock(10, TimeUnit.MILLISECONDS);
        assert !success : "lock was acquired successfully - this is incorrect";

        printLocks(a,b);
        a.getProtocolStack().removeProtocol(DropGrantResponse.class);
    }

    protected static void printLocks(JChannel... channels) {
        for(JChannel ch: channels) {
            Locking l=ch.getProtocolStack().findProtocol(Locking.class);
            System.out.printf("**** server locks on %s: %s\n", ch.getAddress(), l.printServerLocks());
        }
    }

    /**
     * To be inserted on the coord (A): drops the first LOCK_GRANTED response (but queues it), then sends the queued
     * LOCK_GRANTED as response to the next GRANT_LOCK request
     */
    protected static class DropGrantResponse extends Protocol {
        protected final BlockingQueue<Message> lock_granted_requests=new ArrayBlockingQueue<>(1);

        public Object down(Message msg) {
            Locking lock_prot=(Locking)up_prot;
            short lock_prot_id=ClassConfigurator.getProtocolId(lock_prot.getClass());

            Locking.LockingHeader hdr=msg.getHeader(lock_prot_id);
            if(hdr != null) {
                try {
                    Locking.Request req=Util.streamableFromBuffer(Locking.Request::new, msg.getArray(), msg.getOffset(), msg.getLength());
                    switch(req.getType()) {
                        case LOCK_GRANTED:
                            boolean added=lock_granted_requests.offer(msg);
                            if(added)
                                System.out.printf("==> queued the LOCK_GRANTED response to be sent %s\n", req);
                            else {
                                // send the queued LOCK_GRANTED response
                                Message lock_granted_req=lock_granted_requests.peek();
                                System.out.println("==> sending the queued LOCK_GRANTED response");
                                down_prot.down(lock_granted_req);
                                lock_granted_req=null;
                            }
                            return null;
                    }
                }
                catch(Exception ex) {
                    log.error("failed deserializing request", ex);
                }
            }
            return down_prot != null? down_prot.down(msg) : null;
        }

    }
}
