package org.jgroups.blocks;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;

/**
 * Testcase for the DistributedLockManager
 * 
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: DistributedLockManagerTest.java,v 1.6 2008/04/08 07:19:08 belaban Exp $
 */
public class DistributedLockManagerTest extends ChannelTestBase {

    public DistributedLockManagerTest(String testName) {
            super(testName);
    }

    public static Test suite() {
            return new TestSuite(DistributedLockManagerTest.class);
    }

    
    private Channel channel1;
    private Channel channel2;

    protected VotingAdapter adapter1;
    protected VotingAdapter adapter2;
    
    protected LockManager lockManager1;
    protected LockManager lockManager2;

    protected static  boolean logConfigured;

    public void setUp() throws Exception {
        ;
        channel1=createChannel("A");
        adapter1=new VotingAdapter(channel1);
        channel1.connect("voting");


        lockManager1=new DistributedLockManager(adapter1, "1");

        // give some time for the channel to become a coordinator
        try {
            Thread.sleep(1000);
        }
        catch(Exception ex) {
        }

        channel2=createChannel("A");
        adapter2=new VotingAdapter(channel2);
        lockManager2=new DistributedLockManager(adapter2, "2");

        channel2.connect("voting");

        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException ex) {
        }
    }

    public void tearDown() throws Exception {
    	;
        channel2.close();
        
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }
        
        
        channel1.close();
    }

    public void test() throws Exception {
        lockManager1.lock("obj1", "owner1", 10000);
        
        try {
            lockManager1.lock("obj1", "owner2", 10000);
            assertTrue("obj1 should not be locked.", false);
        } catch (LockNotGrantedException ex) {
            // everything is ok
        }
        
        lockManager2.lock("obj2", "owner2", 1000);
        
        lockManager1.unlock("obj1", "owner1");
        
        try {
            lockManager1.unlock("obj2", "owner1");
            assertTrue("obj2 should not be released.", false);
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
        Object acquireLockDecree = acquireLockDecreeConstructor.newInstance(new Object[] {"obj1", "owner2", "2"});
        
        Field heldLocksField = lockManager2.getClass().getDeclaredField("heldLocks");
        heldLocksField.setAccessible(true);
        HashMap heldLocks = (HashMap)heldLocksField.get(lockManager2);
        heldLocks.put("obj1", acquireLockDecree);
        
        // Both lockManagers hold a lock on obj1 now

        try {
            lockManager1.unlock("obj1", "owner1", true);
            assertTrue("obj1 should throw a lockMultiLockedException upon release.", false);
        } catch (LockMultiLockedException e) {
            // everything is ok
        }
        
        try {
            lockManager1.lock("obj1", "owner1", 10000);
            assertTrue("obj1 should throw a LockNotGrantedException because it is still locked by lockManager2.", false);
        } catch (LockNotGrantedException e) {
            // everything is ok
        }
        
        try {
            lockManager2.unlock("obj1", "owner2", true);
            assertTrue("obj1 should throw a lockMultiLockedException upon release.", false);
        } catch (LockMultiLockedException e) {
            // everything is ok
        }
        
        // Everything should be unlocked now
        try {
            lockManager1.lock("obj1", "owner1", 10000);
        } catch (LockNotGrantedException e) {
            assertTrue("obj1 should be unlocked", false);
        }

        lockManager1.unlock("obj1", "owner1", true);
    }

    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }
}
