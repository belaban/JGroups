package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests https://issues.jboss.org/browse/JGRP-1675
 * @author Bela Ban
 * @since  3.5
 */
public class RemoteGetStressTest {
    protected JChannel[]                  channels;
    protected List<Address>               target_members; // B, C, D
    protected RpcDispatcher[]             dispatchers;
    protected Random random = new Random();

    protected static final int            NUM_THREADS=40;
    protected static final int            SIZE=100 * 1000; // size of a GET response
    protected static final byte[]         BUF=new byte[SIZE];
    protected static final MethodCall     GET_METHOD;
    protected static final long           TIMEOUT=3 * 60 * 1000; // ms
    protected static final AtomicInteger  count=new AtomicInteger(1);
    protected static final RequestOptions OPTIONS=RequestOptions.SYNC().timeout(TIMEOUT)
      .flags(Message.Flag.OOB).anycasting(true);
    protected static boolean              USE_SLEEPS=true;

    static {
        try {
            Method get_method=RemoteGetStressTest.class.getMethod("get");
            GET_METHOD=new MethodCall(get_method);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    protected void start() throws Exception {
        String[] names={"A", "B", "C", "D"};
        channels=new JChannel[4];
        dispatchers=new RpcDispatcher[channels.length];
        for(int i=0; i < channels.length; i++) {
            channels[i]=createChannel(names[i]);
            dispatchers[i]=new RpcDispatcher(channels[i], this);
            channels[i].connect("cluster");
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        System.out.println("view A: " + channels[0].getView());

        target_members=Arrays.asList(channels[1].getAddress(), channels[2].getAddress(), channels[3].getAddress());
        final AtomicInteger success=new AtomicInteger(0), failure=new AtomicInteger(0);

        if(USE_SLEEPS)
            insertDISCARD(channels[0], 0.2);

        long start=System.currentTimeMillis();
        Invoker[] invokers=new Invoker[NUM_THREADS];
        for(int i=0; i < invokers.length; i++) {
            invokers[i]=new Invoker(dispatchers[0], success, failure);
            invokers[i].start();
        }

        for(Invoker invoker: invokers)
            invoker.join();

        long time=System.currentTimeMillis() - start;

        System.out.println("\n\n**** success: " + success + ", failure=" + failure + ", time=" + time + " ms");
        Util.keyPress("enter to terminate");
        stop();
    }

    protected void stop() {
        for(RpcDispatcher disp: dispatchers)
            disp.stop();
        Util.close(channels);
    }

    protected static JChannel createChannel(String name) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK().setThreadPoolMinThreads(1).setThreadPoolMaxThreads(5),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new UFC(),
          new MFC().setMaxCredits(2000000).setMinThreshold(0.4),
          new FRAG2().setFragSize(8000),
        };
        return new JChannel(protocols).name(name);
    }

    public static BigObject get() {
        return new BigObject(count.getAndIncrement());
    }


    public static void main(String[] args) throws Exception {
        RemoteGetStressTest test=new RemoteGetStressTest();
        test.start();
    }

    protected Address randomMember() {
        return Util.pickRandomElement(target_members);
    }

    protected static void insertDISCARD(JChannel ch, double discard_rate) throws Exception {
        TP transport=ch.getProtocolStack().getTransport();
        DISCARD discard=new DISCARD();
        discard.setUpDiscardRate(discard_rate);
        ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, transport.getClass());
    }

    protected class Invoker extends Thread {
        protected final RpcDispatcher disp;
        protected final AtomicInteger success, failure;

        public Invoker(RpcDispatcher disp, AtomicInteger success, AtomicInteger failure) {
            this.disp=disp;
            this.success=success;
            this.failure=failure;
        }

        public void run() {
            ArrayList<Address> targets = new ArrayList<>(channels[0].getView().getMembers());
            targets.remove(random.nextInt(targets.size()));
            Collections.rotate(targets, random.nextInt(targets.size()));

            Future<BigObject>[] futures = new Future[targets.size()];
            for(int i=0; i < targets.size(); i++) {
                Address target=targets.get(i);
                try {
                    futures[i]=disp.callRemoteMethodWithFuture(target, GET_METHOD, OPTIONS);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }

            for(Future<BigObject> future: futures) {
                if(future == null) continue;
                try {
                    BigObject result=future.get(TIMEOUT, TimeUnit.MILLISECONDS);
                    if(result != null) {
                        System.out.println("received object #" + result.num);
                        success.incrementAndGet();
                        return;
                    }
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            failure.incrementAndGet();
        }
    }

    public static class BigObject implements Serializable {
        private static final long serialVersionUID=1265292900051224502L;
        int num;

        public BigObject(int num) {
            this.num = num;
        }

        public BigObject() {
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
            byte[] buf = new byte[SIZE];
            out.write(buf);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            byte[] buf = new byte[SIZE];
            in.read(buf);
            if(USE_SLEEPS)
                Util.sleepRandom(1, 10);
        }

        @Override
        public String toString() {
            return "BigObject#" + num;
        }
    }

}
