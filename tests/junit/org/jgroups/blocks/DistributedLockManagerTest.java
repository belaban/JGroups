package org.jgroups.blocks;

import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * Testcase for the DistributedLockManager
 * 
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: DistributedLockManagerTest.java,v 1.9 2008/04/21 07:39:22 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class DistributedLockManagerTest extends ChannelTestBase {
    private Channel channel1;
    private Channel channel2;

    protected VotingAdapter adapter1;
    protected VotingAdapter adapter2;
    
    protected LockManager lockManager1;
    protected LockManager lockManager2;


    @BeforeClass
    void setUp() throws Exception {
        channel1=createChannel(true);
        adapter1=new VotingAdapter(channel1);
        final String props=channel1.getProperties();
        channel1.connect("DistributedLockManagerTest");

        lockManager1=new DistributedLockManager(adapter1, "1");
        Util.sleep(1000);  // give some time for the channel to become a coordinator

        channel2=createChannelWithProps(props);
        adapter2=new VotingAdapter(channel2);
        lockManager2=new DistributedLockManager(adapter2, "2");

        channel2.connect("DistributedLockManagerTest");
        Util.sleep(1000);
    }


    @AfterClass
    void tearDown() throws Exception {
        channel2.close();
        channel1.close();
    }


    public void test() throws Exception {
        lockManager1.lock("obj1", "owner1", 10000);
        
        try {
            lockManager1.lock("obj1", "owner2", 10000);
            throw new IllegalStateException("obj1 should not be locked");
        }
        catch (LockNotGrantedException ex) {
            // everything is ok
        }
        
        lockManager2.lock("obj2", "owner2", 1000);
        lockManager1.unlock("obj1", "owner1");
        
        try {
            lockManager1.unlock("obj2", "owner1");
            throw new IllegalStateException("obj2 should not be released");
        }
        catch (LockNotReleasedException ex) {
            // everything is ok
        }
        
        lockManager1.unlock("obj2", "owner2");
    }

    
    public void testMultiLock() throws Exception {
        lockManager1.lock("obj1", "owner1", 10000);
        
        // Override private members and simulate the errorcase which is, when two lockManagers have locked the same object
        // This can happen after a merge
        Class acquireLockDecreeClass = Class.forName("org.jgroups.blocks.DistributedLockManager$AcquireLockDecree");
        Constructor acquireLockDecreeConstructor = acquireLockDecreeClass.getDeclaredConstructor(new Class[] {Object.class, Object.class, Object.class});
        acquireLockDecreeConstructor.setAccessible(true);
        Object acquireLockDecree = acquireLockDecreeConstructor.newInstance("obj1", "owner2", "2");
        
        Field heldLocksField = lockManager2.getClass().getDeclaredField("heldLocks");
        heldLocksField.setAccessible(true);
        Map<String,Object> heldLocks = (Map<String,Object>)heldLocksField.get(lockManager2);
        heldLocks.put("obj1", acquireLockDecree);
        
        // Both lockManagers hold a lock on obj1 now

        try {
            lockManager1.unlock("obj1", "owner1", true);
            throw new IllegalStateException("obj1 should throw a lockMultiLockedException upon release");
        } catch (LockMultiLockedException e) {
            // everything is ok
        }
        
        try {
            lockManager1.lock("obj1", "owner1", 10000);
            throw new IllegalStateException("obj1 should throw a LockNotGrantedException because it is still locked by lockManager2");
        } catch (LockNotGrantedException e) {
            // everything is ok
        }
        
        try {
            lockManager2.unlock("obj1", "owner2", true);
            throw new IllegalStateException("obj1 should throw a lockMultiLockedException upon release");
        } catch (LockMultiLockedException e) {
            // everything is ok
        }
        
        // Everything should be unlocked now
        try {
            lockManager1.lock("obj1", "owner1", 10000);
        }
        catch (LockNotGrantedException e) {
            throw new IllegalStateException("obj1 should be unlocked");
        }
        lockManager1.unlock("obj1", "owner1", true);
    }

}
