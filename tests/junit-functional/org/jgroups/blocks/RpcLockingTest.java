package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

@Test(groups = {Global.FUNCTIONAL,Global.EAP_EXCLUDED}, singleThreaded=true, dataProvider="createLockingProtocol")
public class RpcLockingTest {
	protected JChannel            a, b;
	protected MessageDispatcher   disp_a, disp_b;
	protected Lock                lock_a, lock_b;



    @DataProvider(name="createLockingProtocol")
    Object[][] createLockingProtocol() {
        return new Object[][] {
          {CENTRAL_LOCK.class},
          {CENTRAL_LOCK2.class}
        };
    }

	protected void setUp(Class<? extends Locking> locking_class) throws Exception {
		System.out.print("Connecting channels: ");
        a=createChannel("A", locking_class);
        disp_a=new MessageDispatcher(a);
        a.connect(RpcLockingTest.class.getSimpleName());
        lock_a=new LockService(a).getLock("lock");

        b=createChannel("B", locking_class);
        disp_b=new MessageDispatcher(b);
        b.connect(RpcLockingTest.class.getSimpleName());
        lock_b=new LockService(b).getLock("lock");

		Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b);
		System.out.println("");

        disp_a.setRequestHandler(arg0 -> {
            System.out.println("A received a message, will now try to lock the lock");
            if(lock_a.tryLock()) {
                Assert.fail("Should not be able to lock the lock here");
                System.out.println("A aquired the lock, this shouldn't be possible");
            }
            else
                System.out.println("The lock was already locked, as it should be");
            return "Hello";
        });

        disp_b.setRequestHandler(arg0 -> {
            System.out.println("B received a message, will now try to lock the lock");
            if(lock_b.tryLock()) {
                Assert.fail("Should not be able to lock the lock here");
                System.out.println("B aquired the lock, this shouldn't be possible");
            }
            else
                System.out.println("The lock already was locked, as it should be");
            return "Hello";
        });

        // Print who is the coordinator
        if (b.getView().getMembers().get(0).equals(b.getAddress()))
            System.out.println("B is the coordinator");
        else
            System.out.println("A is the coordinator");
        System.out.println("");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(b,a);
    }

    protected static JChannel createChannel(String name, Class<? extends Locking> locking_class) throws Exception {
        return new JChannel(
          new SHARED_LOOPBACK(), new SHARED_LOOPBACK_PING(),
          new MERGE3().setValue("min_interval", 1000).setValue("max_interval", 3000),
          new NAKACK2().setValue("use_mcast_xmit", false).setValue("discard_delivered_msgs", true)
            .setValue("log_discard_msgs", false).setValue("log_not_found_msgs", false),
          new UNICAST3().setValue("xmit_table_num_rows", 5).setValue("xmit_interval", 500),
          new GMS().joinTimeout(1000).setValue("print_local_addr", false).setValue("leave_timeout", 100)
            .setValue("log_view_warnings", false).setValue("view_ack_collection_timeout", 2000)
            .setValue("log_collect_msgs", false),
          locking_class.getDeclaredConstructor().newInstance())
          .name(name);
    }



	/**
	 * If the coordinator of the lock locks the lock and then send a message,
	 * the receiver will wait for ever in tryLock. However, castMessage will
	 * return after a while because of the default settings of RequestOptions.SYNC().
	 */
	public void testCoordSendFirst(Class<? extends Locking> locking_class) throws Exception {
	    setUp(locking_class);
		System.out.println("Running testCoordSendFirst");

		// ===========================================================================
        if (lock_a.tryLock()) {
            try {
                System.out.println("A aquired the lock, about to send message to B");
                byte[] buf="bla".getBytes();
                String rsp=disp_a.sendMessage(b.getAddress(), buf, 0, buf.length, RequestOptions.SYNC().timeout(60000).flags(Message.Flag.OOB));
                if (rsp == null) {
                    System.err.println("ERROR: didn't return correctly");
                    Assert.fail("Didn't return correctly");
                } else
                    System.out.println("Returned: " + rsp);

            } finally {
                lock_a.unlock();
            }
        } else {
            Assert.fail("The lock was already locked");
            System.out.println("A failed to aquire the lock");
        }
        // ===========================================================================

		System.out.println();
	}

	/**
	 * If the node that isn't the coordinator is the one who sends the message
	 * it works, but later when the coordinator sends the message, the receiver, will wait forever in tryLock.
	 */
	public void testCoordReceiveFirst(Class<? extends Locking> locking_class) throws Exception {
	    setUp(locking_class);
		System.out.println("Running testCoordReceiveFirst");

		if(lock_b.tryLock()) {
			try {
				System.out.println("B aquired the lock, about to send message to A");
                byte[] buf="bla".getBytes();
				String rsp = disp_b.sendMessage(a.getAddress(), buf, 0, buf.length, RequestOptions.SYNC().flags(Message.Flag.OOB));
				if (rsp == null) {
                    System.err.println("ERROR: didn't return correctly");
					Assert.fail("Didn't return correctly");
				} else
					System.out.println("Returned: " + rsp);

			} finally {
				lock_b.unlock();
			}
		} else {
			Assert.fail("The lock was already locked");
			System.out.println("B failed to aquire the lock");
		}
		// ===========================================================================

		if(lock_a.tryLock(5000, TimeUnit.MILLISECONDS)) {
			try {
				System.out.println("A aquired the lock, about to send message to B");
                byte[] buf="bla".getBytes();
                String rsp = disp_a.sendMessage(b.getAddress(), buf, 0, buf.length, RequestOptions.SYNC().timeout(60000).flags(Message.Flag.OOB));
				if (rsp == null) {
					System.err.println("ERROR: didn't return correctly");
					Assert.fail("Didn't return correctly");
				}
                else
                    System.out.println("Returned: " + rsp);
			} finally {
				lock_a.unlock();
			}
		} else {
			Assert.fail("The lock was already locked");
			System.out.println("A failed to aquire the lock");
		}
		// ===========================================================================
		System.out.println();

	}

    protected void enableTracing() {
        for(JChannel ch: Arrays.asList(a,b))
            ch.getProtocolStack().findProtocol(Locking.class).setLevel("TRACE");
    }


}
