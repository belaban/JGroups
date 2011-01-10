package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.AbstractLockService;
import org.jgroups.blocks.locking.PeerLockService;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.locks.Lock;

/** Tests {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class LockServiceTest extends ChannelTestBase {
    protected JChannel c1, c2, c3, c4;
    protected static final String LOCK="sample-lock";


    @BeforeTest
    protected void init() throws Exception {
        c1=createChannel(true, 4, "A");
        c1.connect("LockServiceTest");

        c2=createChannel(c1, "B");
        c2.connect("LockServiceTest");

        c3=createChannel(c1, "C");
        c3.connect("LockServiceTest");

        c4=createChannel(c1, "D");
        c4.connect("LockServiceTest");
    }

    @AfterTest
    protected void cleanup() {
        Util.close(c4,c3,c2,c1);
    }

    @DataProvider(name="createLockService")
    AbstractLockService[][] createLockService() {
        return new AbstractLockService[][] {
          {new PeerLockService(), new PeerLockService(), new PeerLockService(), new PeerLockService()}
        };
    }

    @Test(dataProvider="createLockService")
    public void testSimpleLock(AbstractLockService s1, AbstractLockService s2, AbstractLockService s3, AbstractLockService s4) {
        s1.setChannel(c1);
        s2.setChannel(c2);
        s3.setChannel(c3);
        s4.setChannel(c4);

        
        Lock lock=s1.getLock(LOCK);

        System.out.print("acquiring lock " + LOCK + ": ");
        lock.lock();
        try {
            System.out.println("OK");
            Util.sleep(1000);
            System.out.print("releasing lock " + LOCK + ": ");
        }
        finally {
            lock.unlock();
            System.out.println("OK");
        }
    }

    @Test(dataProvider="createLockService")
    public void testBlockingLock(AbstractLockService s1, AbstractLockService s2, AbstractLockService s3, AbstractLockService s4) {
        s1.setChannel(c1);
        s2.setChannel(c2);
        s3.setChannel(c3);
        s4.setChannel(c4);


        final Lock l1=s1.getLock(LOCK);
        System.out.println("locking l1");
        l1.lock();
        System.out.println("locked l1");

         new Thread() {
            public void run() {
                Util.sleep(5000);
                System.out.println("thread is unlocking l1");
                l1.unlock();
            }
        }.start();

        try {
            Lock l2=s2.getLock(LOCK);
            System.out.println("locking l2");
            l2.lock();
            System.out.println("locked l2");
            try {
                ;
            }
            finally {
                l2.unlock();
                System.out.println("unlocked l2");
            }
        }
        finally {
            l1.unlock();
        }

    }
}
