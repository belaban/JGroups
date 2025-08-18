package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Misc tests for {@link org.jgroups.protocols.JDBC_PING2}. The postgrsql DB needs to be running and its driver JAR
 * needs to be on the classpath
 * @author Bela Ban
 * @since  5.4, 5.3.7
 */
@Test(groups=Global.JDBC,singleThreaded=true)
public class JDBC_PING2_Test {
    protected static final String CLUSTER="jdbc-test";
    protected static final int NUM_NODES=8;

    public void testClusterFormedAfterRestart() throws Exception {
        try(var a=createChannel("jdbc-pg.xml", "A")) {
            a.connect(CLUSTER);
            for(int i=1; i <= 1000; i++) {
                long start=System.nanoTime();
                try(var b=createChannel("jdbc-pg.xml", "B")) {
                    b.connect(CLUSTER);
                    Util.waitUntilAllChannelsHaveSameView(10000, 10, a,b);
                    long time=System.nanoTime()-start;
                    System.out.printf("-- join #%d took %s\n", i, Util.printTime(time, TimeUnit.NANOSECONDS));
                }
            }
        }
    }

    public void testConcurrentStartup() throws Exception {
        JChannel[] channels=new JChannel[NUM_NODES];
        for(int i=0; i < channels.length; i++)
            channels[i]=createChannel("jdbc-pg.xml", String.valueOf(i+1));
        CountDownLatch latch=new CountDownLatch(1);
        int index=1;
        for(JChannel ch: channels) {
            ThreadFactory thread_factory=ch.stack().getTransport().getThreadFactory();
            Connector connector=new Connector(latch, ch);
            thread_factory.newThread(connector, "connector-" +index++).start();
        }
        latch.countDown();
        long start=System.nanoTime();
        Util.waitUntilAllChannelsHaveSameView(30000, 100, channels);
        long time=System.nanoTime()-start;
        System.out.printf("-- cluster of %d formed in %s:\n%s", NUM_NODES, Util.printTime(time),
                          Stream.of(channels).map(ch -> String.format("%s: %s", ch.address(), ch.view()))
                            .collect(Collectors.joining("\n")));
    }

    protected static JChannel modify(JChannel ch) {
        GMS gms=ch.stack().findProtocol(GMS.class);
        gms.setJoinTimeout(3000).setMaxJoinAttempts(5);
        MERGE3 merge=ch.stack().findProtocol(MERGE3.class);
        merge.setMinInterval(2000).setMaxInterval(5000);
        return ch;
    }

    protected static JChannel createChannel(String cfg, String name) throws Exception {
        return modify(new JChannel(cfg).name(name));
    }

    protected static class Connector implements Runnable {
        protected final CountDownLatch latch;
        protected final JChannel       ch;

        protected Connector(CountDownLatch latch, JChannel ch) {
            this.latch=latch;
            this.ch=ch;
        }

        @Override
        public void run() {
            try {
                latch.await();
                ch.connect(CLUSTER);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
