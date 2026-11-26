package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.logging.Log;
import org.jgroups.protocols.UDP;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Tests {@link MaxOneThreadPerSender}
 * @author Radoslav Husar
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MaxOneThreadPerSenderTest {
    protected final List<Message> list=new FastArray<>(5);

    @BeforeMethod protected void init() {list.clear();}

    /**
     * A BatchHandlerLoop which terminates abnormally (here: because logging the failure fails, too) must not leave
     * the entry with running=true, or else messages from that sender are queued forever and never delivered
     */
    public void testRunningIsResetOnAbnormalTermination() throws Exception {
        UDP tp=new UDP() {
            @Override public void passBatchUp(MessageBatch b, boolean cluster_name_matching, boolean discard_own_mcast) {
                for(Message msg: b)
                    list.add(msg);
            }

            @Override
            public void passMessageUp(Message msg, boolean perform_cluster_name_matching, boolean multicast, boolean discard_own_mcast) {
                list.add(msg);
                throw new IllegalArgumentException("booom");
            }
        };
        MaxOneThreadPerSender policy=new MaxOneThreadPerSender();
        tp.getThreadPool().init();
        try {
            policy.init(tp);
            policy.log=throwingLog();
            Message msg=new ObjectMessage(null, "hello").setSrc(Util.createRandomAddress("A"));
            assert policy.process(msg, false);
            Util.waitUntil(5000, 100, () -> !list.isEmpty());
            System.out.printf("list: %s\n", list);
        }
        finally {
            tp.getThreadPool().destroy();
        }
    }

    public void testRunningIsResetOnFullThreadPool() throws Exception {
        UDP tp=new UDP() {
            @Override public void passBatchUp(MessageBatch b, boolean cluster_name_matching, boolean discard_own_mcast) {
                for(Message msg: b)
                    list.add(msg);
            }

            @Override
            public void passMessageUp(Message msg, boolean perform_cluster_name_matching, boolean multicast, boolean discard_own_mcast) {
                list.add(msg);
            }
        };
        MaxOneThreadPerSender policy=new MaxOneThreadPerSender();
        ThreadPool thread_pool=tp.getThreadPool().setMinThreads(1).setMaxThreads(5);
        thread_pool.init();
        CountDownLatch latch=new CountDownLatch(1);
        try {
            boolean success;
            do {
                BlockingTask bt=new BlockingTask(latch);
                success=thread_pool.execute(bt);
            }
            while(success);
            policy.init(tp);
            Address sender=Util.createRandomAddress("A");
            Message msg=new ObjectMessage(null, "one").setSrc(sender);
            assert policy.process(msg, false);
            latch.countDown(); // releases all BlockingTasks from the thread pool
            msg=new ObjectMessage(null, "two").setSrc(sender);
            assert policy.process(msg, false);
            MaxOneThreadPerSender.Entry entry=policy.mcasts.map.values().iterator().next();
            Util.waitUntil(5000, 100, () -> list.size() == 2);
            System.out.printf("**list: %s\n", list);
        }
        finally {
            thread_pool.destroy();
        }
    }

    protected static boolean running(MaxOneThreadPerSender.Entry entry) {
        return true; // todo: remove
    }

    protected static class BlockingTask implements Runnable {
        protected final CountDownLatch latch;

        protected BlockingTask(CountDownLatch latch) {
            this.latch=latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Returns a Log which fails when an error is logged, e.g. as an OOME would */
    protected static Log throwingLog() {
        return (Log)Proxy.newProxyInstance(MaxOneThreadPerSenderTest.class.getClassLoader(), new Class<?>[]{Log.class},
                                           (proxy, method, args) -> {
                                               if(method.getName().contains("Error"))
                                                   throw new IllegalStateException("logging failed");
                                               return method.getReturnType() == boolean.class? Boolean.FALSE : null;
                                           });
    }
}
