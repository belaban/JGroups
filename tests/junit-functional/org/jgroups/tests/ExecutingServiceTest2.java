package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.executor.ExecutionRunner;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.protocols.CENTRAL_EXECUTOR;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


/** Tests https://issues.jboss.org/browse/JGRP-1696 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED}, singleThreaded=true)
public class ExecutingServiceTest2 {

    Set<Thread> threads=new HashSet<>();
    Set<Channel> channels=new HashSet<>();

    @AfterMethod
    public void tearDown() {
        for(Thread thread : threads)
            thread.interrupt();
        for(Channel channel : channels)
            channel.close();
    }

    @Test
    public void testDisconnect() throws Exception {
        JChannel channel1=new JChannel(Util.getTestStack(new CENTRAL_EXECUTOR()));
        JChannel channel2=new JChannel(Util.getTestStack(new CENTRAL_EXECUTOR()));
        // channel1.getProtocolStack().addProtocol(new CENTRAL_EXECUTOR());
        // channel2.getProtocolStack().addProtocol(new CENTRAL_EXECUTOR());
        channels.add(channel1);
        channels.add(channel2);

        channel1.connect("test-cluster");
        channel2.connect("test-cluster");
        Util.waitUntilAllChannelsHaveSameView(20000, 1000, channel1, channel2);

        final ExecutionService executionService=new ExecutionService(channel1);
        ExecutionRunner executionRunner1=new ExecutionRunner(channel1);
        ExecutionRunner executionRunner2=new ExecutionRunner(channel2);

        Thread runner1=new Thread(executionRunner1);
        threads.add(runner1);
        runner1.start();
        Thread runner2=new Thread(executionRunner2);
        threads.add(runner2);
        runner2.start();

        final AtomicInteger submittedTasks=new AtomicInteger();
        final AtomicInteger finishedTasks=new AtomicInteger();
        final FutureListener<Void> listener=new FutureListener<Void>() {
            @Override
            public void futureDone(Future<Void> future) {
                finishedTasks.incrementAndGet();
                synchronized(ExecutingServiceTest2.this) {
                    ExecutingServiceTest2.this.notify();
                }
            }
        };

        Thread submitter=new Thread(new Runnable() {
            @Override
            public void run() {
                // Two long running tasks that should be sent to each runner
                submit(true);
                submit(true);
                while(!Thread.interrupted()) {
                    submit(false);

                    // Throttle
                    try {
                        Thread.sleep(50);
                    }
                    catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            private void submit(boolean wait) {
                Callable<Void> task=new Wait(wait);
                NotifyingFuture<Void> future=executionService.submit(task);
                submittedTasks.incrementAndGet();
                future.setListener(listener);
            }

        });
        threads.add(submitter);
        submitter.start();

        // Run for 2 seconds
        Thread.sleep(500);

        // Close channel
        channel2.close();

        // Stop submitting
        submitter.interrupt();
        submitter.join();

        // Wait for running tasks to finish
        synchronized(this) {
            int lastFinished=finishedTasks.get();
            while(submittedTasks.get() > finishedTasks.get()) {
                wait(10000);
                if(lastFinished == finishedTasks.get()) {
                    assert false : "Tasks still outstanding, none finished in the last 10s";
                }
                lastFinished=finishedTasks.get();
            }
        }

        Assert.assertEquals(submittedTasks.get(),finishedTasks.get(),"Tasks not finished");
    }


    private static final class Wait implements Callable<Void>, Serializable {
        boolean wait=false;

        Wait(boolean wait) {
            super();
            this.wait=wait;
        }


        @Override
        public Void call() {
            if(wait) {
                try {
                    Thread.sleep(5000);
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }
}

