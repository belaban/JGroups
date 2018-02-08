package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.protocols.Locking;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Tests concurrent access to the locks provided by {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups={Global.BYTEMAN,Global.EAP_EXCLUDED},singleThreaded=true)
public class LockServiceConcurrencyTest {
    protected JChannel           a, b;
    protected LockService        ls_a, ls_b;
    protected static final short CENTRAL_LOCK_ID=ClassConfigurator.getProtocolId(CENTRAL_LOCK.class);


    @BeforeMethod protected void init() throws Exception {
        a=new JChannel(Util.getTestStack(new CENTRAL_LOCK())).name("A");
        ls_a=new LockService(a);
        a.connect("LockServiceConcurrencyTest");
        b=new JChannel(Util.getTestStack(new CENTRAL_LOCK())).name("B");
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
    public void testConcurrentClientLocks() throws Exception {
        Lock lock=ls_b.getLock("L"); // A is the coordinator

        DropGrantResponse dropper=new DropGrantResponse();
        a.getProtocolStack().insertProtocol(dropper, ProtocolStack.Position.BELOW, CENTRAL_LOCK.class);

        // we're dropping the LOCK-GRANTED response for lock-id #1, so this lock acquisition must fail; lock L will not be released!
        boolean success=lock.tryLock(1, TimeUnit.MILLISECONDS);
        assert !success : "the lock acquisition should have failed";


        // the LOCK-GRANTED response for lock-id #2 is received, which is incorrect and therefore dropped
        // tryLock() works the same, with or without timeout
        success=lock.tryLock(10, TimeUnit.MILLISECONDS); // timeout needs to be greater than 1 (byteman rule fires on this) !
        assert !success : "lock was acquired successfully - this is incorrect";

        printLocks(a,b);
        a.getProtocolStack().removeProtocol(DropGrantResponse.class);
    }

    protected void printLocks(JChannel ... channels) {
        for(JChannel ch: channels) {
            Locking l=ch.getProtocolStack().findProtocol(CENTRAL_LOCK.class);
            System.out.printf("**** server locks on %s: %s\n", ch.getAddress(), l.printServerLocks());
        }
    }

    /**
     * To be inserted on the coord (A): drops the first LOCK_GRANTED response (but queues it), then sends the queued
     * LOCK_GRANTED as response to the next GRANT_LOCK request
     */
    protected static class DropGrantResponse extends Protocol {
        protected int     num_grant_lock_reqs_received;
        protected Message lock_granted_req;


        public Object down(Message msg) {
            Locking.LockingHeader hdr=msg.getHeader(CENTRAL_LOCK_ID);
            if(hdr != null) {
                try {
                    Locking.Request req=Util.streamableFromBuffer(Locking.Request::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    switch(req.getType()) {
                        case LOCK_GRANTED:
                            if(num_grant_lock_reqs_received == 1) {
                                lock_granted_req=msg;
                                System.out.printf("==> queued the LOCK_GRANTED response to be sent %s\n", req);
                                return null; // drops the LOCK_GRANTED response and queues the response
                            }
                            if(num_grant_lock_reqs_received == 2) {
                                // send the queued LOCK_GRANTED response
                                if(lock_granted_req != null) {
                                    System.out.printf("==> sending the queued LOCK_GRANTED response\n");
                                    down_prot.down(lock_granted_req);
                                    lock_granted_req=null;
                                    return null; // and drop the real LOCK_GRANTED response
                                }
                            }
                            break;
                    }
                }
                catch(Exception ex) {
                    log.error("failed deserializing request", ex);
                }
            }
            return down_prot.down(msg);
        }

        public Object up(Message msg) {
            handleMessage(msg);
            return up_prot.up(msg);
        }

        public void up(MessageBatch batch) {
            for(Message msg: batch)
                handleMessage(msg);
            if(!batch.isEmpty())
                up_prot.up(batch);
        }

        protected void handleMessage(Message msg) {
            Locking.LockingHeader hdr=msg.getHeader(CENTRAL_LOCK_ID);
            if(hdr != null) {
                try {
                    Locking.Request req=Util.streamableFromBuffer(Locking.Request::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    switch(req.getType()) {
                        case GRANT_LOCK:
                            ++num_grant_lock_reqs_received;
                            System.out.printf("==> received GRANT_LOCK request #%d\n", num_grant_lock_reqs_received);
                            break;
                    }
                }
                catch(Exception ex) {
                    log.error("failed deserializing request", ex);
                }
            }
        }
    }
}
