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
    protected static final String LOCK="lock";


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
          {new PeerLockService(), new PeerLockService()}
        };
    }

    @Test(dataProvider="createLockService")
    public void testSimpleLock(AbstractLockService s1, AbstractLockService s2) {
        s1.setChannel(c1);
        s2.setChannel(c2);
        
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
}
