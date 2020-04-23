package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests concurrent startup with a bunch of different local discovery protocols, such as
 * {@link org.jgroups.protocols.SHARED_LOOPBACK} / {@link org.jgroups.protocols.SHARED_LOOPBACK_PING} and
 * {@link org.jgroups.protocols.UDP} / {@link org.jgroups.protocols.LOCAL_PING}.
 * @author Bela Ban
 * @since  4.1.8
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ConcurrentStartupTest {
    protected static final String CLUSTER_SHARED=ConcurrentStartupTest.class.getSimpleName() + "-SHARED";
    protected static final String CLUSTER_LOCAL=ConcurrentStartupTest.class.getSimpleName() + "-LOCAL";
    protected static final int    NUM_CHANNELS=10;
    protected static JChannel[]   channels=new JChannel[NUM_CHANNELS];

    protected static Log log=LogFactory.getLog(GMS.class);

    @AfterMethod protected void destroy() throws TimeoutException {
        Util.closeFast(channels);
        Util.waitUntil(5000, 500, () -> Stream.of(channels).allMatch(JChannel::isClosed));
    }

    @Test(enabled=false)
    protected void setup(Class<? extends TP> tp_cl, Class<? extends Discovery> discovery_cl) throws Exception {
        for(int i=0; i < channels.length; i++)
            channels[i]=create(tp_cl, discovery_cl, String.valueOf(i+1));
    }


    // @Test(invocationCount=5)
    public void testConcurrentJoinWithSHARED_LOOPBACK() throws Exception {
        setup(SHARED_LOOPBACK.class, SHARED_LOOPBACK_PING.class);
        startThreads(CLUSTER_SHARED);
    }

    @Test(invocationCount=10)
    public void testConcurrentJoinWithLOCAL_PING() throws Exception {
        setup(UDP.class, LOCAL_PING.class);
        for(int i=0; i < channels.length; i++) {
            final int index=i;
            channels[i].setUpHandler(new UpHandler() {
                boolean first_view_received;

                public Object up(Message msg) {return null;}

                public Object up(Event evt) {
                    if(evt.getType() == Event.VIEW_CHANGE) {
                        if(!first_view_received) {
                            first_view_received=true;
                            long sleep_time=Util.random(100);
                            System.out.printf("%s: sleeping for %d ms\n", channels[index].getAddress(), sleep_time);
                            Util.sleep(sleep_time);
                        }
                    }
                    return null;
                }
            });
        }
        startThreads(CLUSTER_LOCAL);
    }

    // @Test(invocationCount=10)
    public void testConcurrentJoinWithPING() throws Exception {
        setup(UDP.class, PING.class);
        startThreads("withUDPandPING");
    }

    protected static JChannel create(Class<? extends TP> tp_cl, Class<? extends Discovery> discovery_cl,
                                     String name) throws Exception {
        Protocol[] protocols={
          tp_cl.getDeclaredConstructor().newInstance().setBindAddress(Util.getLoopback()),
          discovery_cl.getDeclaredConstructor().newInstance(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS().setJoinTimeout(1000).setLeaveTimeout(100)
        };
        return new JChannel(protocols).name(name);
    }

    @Test(enabled=false)
    protected void startThreads(String cluster) throws TimeoutException {
        final CountDownLatch latch=new CountDownLatch(1);
        Joiner[] joiners=new Joiner[channels.length];
        for(int i=0; i < channels.length; i++) {
            joiners[i]=new Joiner(latch, channels[i], cluster);
            joiners[i].start();
        }
        System.out.printf("Starting parallel join of %d channels\n", channels.length);
        latch.countDown();
        Util.waitUntilAllChannelsHaveSameView(5000, 500, channels);
        System.out.printf("\nAll channels have the same views:\n%s\n",
                          Stream.of(channels).map(ch -> String.format("%s: %s", ch.getAddress(), ch.getView()))
                            .collect(Collectors.joining("\n")));
        Util.waitUntil(50000, 1000, () -> Stream.of(joiners).noneMatch(Thread::isAlive));
    }


    protected static class Joiner extends Thread {
        protected final CountDownLatch latch;
        protected final JChannel       ch;
        protected final String         cluster;

        public Joiner(CountDownLatch latch, JChannel ch, String cluster) {
            this.latch=latch;
            this.ch=ch;
            this.cluster=cluster;
        }

        public void run() {
            try {
                latch.await(10, TimeUnit.SECONDS);
                ch.connect(cluster);
            }
            catch(Exception e) {
            }
        }
    }
}
