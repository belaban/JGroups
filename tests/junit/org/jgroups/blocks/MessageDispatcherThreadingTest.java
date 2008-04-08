package org.jgroups.blocks;


import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.tests.MessageDispatcherTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/*
 * Created on May 16, 2005
 */

/**
 * @author BConlon
 * @version $Revision: 1.3 $
 */
public class MessageDispatcherThreadingTest extends ChannelTestBase {

    Channel channel = null;
    //~ Methods ----------------------------------------------------------------------------------------------

    public void setUp() throws Exception {
        ;      
        channel = createChannel();
    }
    
    public void tearDown() throws Exception {        
        if(channel != null){
            channel.close();
        }

        ;
    }
    
    public void testAllThreadsAreStopped() throws Exception {
        // get the threads for this thread group
        ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        Thread[] originalThreads = new Thread[threadGroup.activeCount()];

        // populate the array with the threads in this group.
        threadGroup.enumerate(originalThreads);
        Set<Thread> originalThreadSet = new HashSet<Thread>(Arrays.asList(originalThreads));

        new MessageDispatcherTest(createChannel()).start();

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

