package org.jgroups.tests;

import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Tests pinned threads (JEP-491).
 * <p>
 * If we have more pinned virtual threads than the parallelism of the ForkJoinPool (VirtualThread.scheduler), then
 * the virtual worker threads will never get to run because the FJP is exhaused, since all threads are blocked.
 * <p>
 * If -cores is higher than the number of pinned threads (even by one), and all the workers are non-pinned, then the
 * test will succeed, and all worker threads will complete.
 * @author Bela Ban
 * @since  5.4.4
 */
public class PinnedVThreads {
    protected int  cores, pinned;
    protected long timeout=5000;

    public int  cores()            {return cores;}
    public void cores(int cores)   {this.cores=cores;}
    public int  pinned()           {return pinned;}
    public void pinned(int pinned) {this.pinned=pinned;}
    public long timeout()          {return timeout;}
    public void timeout(long t)    {timeout=t;}


    public void start() throws IOException, InterruptedException {
        int available_cores=Runtime.getRuntime().availableProcessors();
        if(cores > 0) {
            String s=String.valueOf(cores);
            System.setProperty("jdk.virtualThreadScheduler.parallelism", s);
            System.out.printf("-- set FJP parallelism to %d cores (available: %d)\n", cores, available_cores);
        }
        else
            System.out.printf("-- FJP parallelism will use %d cores\n", available_cores);
        System.out.printf("-- %d virtual threads will be pinned\n", pinned);

        ThreadFactory factory=new DefaultThreadFactory("test", true, false).useVirtualThreads(true);
        Object[] locks=new Object[pinned];
        for(int i=0; i < pinned; i++) {
            locks[i]=new Object();
            final Object lock=locks[i];
            Thread t=factory.newThread(() -> {
                synchronized(lock) {
                    System.out.printf("[%s] parking\n", Thread.currentThread());
                    LockSupport.park();
                }
            }, "pinner-" + i);
            t.start();
        }

        List<Thread> workers=new ArrayList<>(pinned);
        AtomicInteger count=new AtomicInteger();
        for(int i=0; i < pinned; i++) {
            Runnable r=() -> doWork(count);
            Thread t=factory.newThread(r, "worker-" + i);
            workers.add(t);
            t.start();
        }

        Util.waitUntilTrue(timeout, 500, () -> count.get() == pinned);
        if(count.get() == pinned)
            System.out.printf("\n** SUCCESS: %d worker threads completed (expected: %d)\n", count.get(), pinned);
        else
            System.err.printf("\n** FAILURE: %d worker threads completed (expected: %d)\n", count.get(), pinned);
    }

    protected static void doWork(AtomicInteger cnt) {
        long start=System.currentTimeMillis();
        Util.sleepRandom(1000, 5000);
        long time=System.currentTimeMillis() - start;
        System.out.printf("[%s] done, slept for %,d ms\n", Thread.currentThread(), time);
        cnt.incrementAndGet();
    }

    public static void main(String[] args) throws Exception {
        PinnedVThreads test=new PinnedVThreads();
        for(int i=0; i < args.length; i++) {
            if("-cores".equals(args[i])) {
                test.cores(Integer.parseInt(args[++i]));
                continue;
            }
            if("-pinned".equals(args[i])) {
                test.pinned(Integer.parseInt(args[++i]));
                continue;
            }
            if("-timeout".equals(args[i])) {
                test.timeout(Long.parseLong(args[++i]));
                continue;
            }
            System.out.printf("%s [-cores <number of cores>] [-pinned <number of virtual threads pinned>] " +
                                "[-timeout <msecs>]\n",
                              PinnedVThreads.class.getSimpleName());
            return;
        }
        test.start();
    }
}
