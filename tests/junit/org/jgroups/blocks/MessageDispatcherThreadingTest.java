package org.jgroups.blocks;

import junit.framework.TestCase;

import org.jgroups.tests.MessageDispatcherTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/*
 * Created on May 16, 2005
 */

/**
 * @author BConlon
 * @version $Revision: 1.1 $
 */
public class MessageDispatcherThreadingTest
    extends TestCase {

    //~ Methods ----------------------------------------------------------------------------------------------

    public void testAllThreadsAreStopped() throws Exception {
        // get the threads for this thread group
        ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        Thread[] originalThreads = new Thread[threadGroup.activeCount()];

        // populate the array with the threads in this group.
        threadGroup.enumerate(originalThreads);
        Set originalThreadSet = new HashSet(Arrays.asList(originalThreads));

        new MessageDispatcherTest().start();

        // wait for anything necessary to finish up.
        Thread.sleep(10000);

        Thread[] currentThreads = new Thread[threadGroup.activeCount()];

        // populate the array with the threads in this group.
        boolean residualThreadsFound = false;
        threadGroup.enumerate(currentThreads);

        // Look for threads not alive when we started.
        for (int i = 0; i < currentThreads.length; i++) {
            Thread thread = currentThreads[i];
            if (! originalThreadSet.contains(thread) && thread.isAlive()) {
                residualThreadsFound = true;
                System.out.println("The following thread was left over: " + thread);
            }
        }
        assertFalse("Residual threads were found", residualThreadsFound);
    }
}

