package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Tests https://issues.redhat.com/browse/JGRP-1429
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.TIME_SENSITIVE,singleThreaded=true)
public class VERIFY_SUSPECT_Test {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");
    protected static final long    VIEW_ACK_COLLECTION_TIMEOUT=10000;
    protected long                 start;


    public void testTimer() {
        VERIFY_SUSPECT ver=new VERIFY_SUSPECT();
        ProtImpl impl=new ProtImpl();
        ver.setUpProtocol(impl).setDownProtocol(new NoopProtocol());

        start=System.currentTimeMillis();
        ver.up(new Event(Event.SUSPECT, Collections.singletonList(a)));

        Util.sleep(100);
        ver.up(new Event(Event.SUSPECT,Collections.singletonList(b)));

        Map<Address,Long> map=impl.getMap();
        Util.waitUntilTrue(10000, 500, () -> map.size() == 2);
        System.out.println("map = " + map);

        long timeout_a=map.get(a), timeout_b=map.get(b);
        assert timeout_a >= 2000 && timeout_a < 2500;
        assert timeout_b >= 2100 && timeout_b < 2500;
    }

    public void testTimer2() throws TimeoutException {
        VERIFY_SUSPECT ver=new VERIFY_SUSPECT();
        ProtImpl impl=new ProtImpl();
        ver.setUpProtocol(impl);
        ver.setDownProtocol(new NoopProtocol());

        start=System.currentTimeMillis();
        ver.up(new Event(Event.SUSPECT, Collections.singletonList(a)));

        Util.sleep(100);
        ver.up(new Event(Event.SUSPECT,Collections.singletonList(b)));

        Map<Address,Long> map=impl.getMap();

        for(int i=0; i < 5; i++) {
            Address addr=Util.createRandomAddress(String.valueOf(i));
            ver.up(new Event(Event.SUSPECT, Collections.singletonList(addr)));
            Util.sleep(100);
        }
        Util.waitUntil(10000, 500, () -> map.size() == 7);
    }

    public void testUnsuspect() throws TimeoutException {
        VERIFY_SUSPECT ver=new VERIFY_SUSPECT().setTimeout(1000);
        ProtImpl impl=new ProtImpl();
        ver.setUpProtocol(impl);
        ver.setDownProtocol(new Protocol() {
            public Object down(Event evt) {
                return null;
            }
            public Object down(Message msg) {return null;}
            public ThreadFactory getThreadFactory() {
                return new DefaultThreadFactory("foo",false,true);
            }
        });

        Map<Address,Long> map=impl.getMap();
        start=System.currentTimeMillis();
        ver.up(new Event(Event.SUSPECT, Collections.singletonList(a)));
        ver.up(new Event(Event.SUSPECT, Collections.singletonList(b)));

        Util.waitUntil(10000, 500, () -> map.size() == 2);
        ver.up(new Event(Event.SUSPECT, Collections.singletonList(a)));
        ver.up(new Event(Event.SUSPECT, Collections.singletonList(b)));
        ver.unsuspect(a);
        Util.waitUntilTrue(10000, 500, () -> map.size() == 1);
        assert map.size() == 1 && map.containsKey(b);
    }

    /**
     * in this case we send two different all suspect events at the same time
     * so it should land in two windows and therefore two events
     */
    public void testMultipleSuspectEventsNextWindow() throws Exception {
        JChannel[] channels = createChannels();
        try {
            CaptureProtocol capture = new CaptureProtocol(2, Event.SUSPECT);
            ProtocolStack stack = channels[0].getProtocolStack();
            stack.insertProtocol(capture, ProtocolStack.Position.ABOVE, VERIFY_SUSPECT2.class);

            Address[] addrs=new Address[channels.length];
            for(int i=0; i < channels.length; i++)
                addrs[i]=channels[i].getAddress();

            VERIFY_SUSPECT2 verify=channels[0].getProtocolStack().findProtocol(VERIFY_SUSPECT2.class);
            TP transport=channels[0].getProtocolStack().getTransport();
            // we ensure different windows
            Suspecter s1=new Suspecter(List.of(addrs[1], addrs[2]), 500, transport), // this will expire at 1.5s (windows 2s)
                    s2=new Suspecter(List.of(addrs[3], addrs[4]), 1200, transport); // this should expire at 2.2 s (windows 3s)
            verify.stop();
            verify.start(); // we ensure we start from T0 = 0s so the s1 get in the first window and s2 in the second
            s1.start();
            s2.start();

            long st=System.nanoTime();
            assert capture.await(5, TimeUnit.SECONDS)
              : String.format("we expected 2 events, but received only %d", capture.count()); // you need to check when the view is 1
            long time=System.nanoTime() - st;
            System.out.printf("waited for %s\n", Util.printTime(time, TimeUnit.NANOSECONDS));
        }
        finally {
            for(JChannel ch: channels)
                Util.shutdown(ch);
        }
    }

    /**
     * {A,B,C,D,E}: at time T, {B,C} are suspected, then at time T+500 {D,E}. This results in 2 views V2={A,D,E}
     * and V3={A}. V2 will run into GMS.view_ack_collection_timeout, as acks from D and E are missing.
     * <br/>
     * Issue: https://issues.redhat.com/browse/JGRP-2556
     * in this case we send two different suspect events in the same window T0 and T0 +500 ms ... 
     * so it should land in one window and therefore one event
     */
    public void testMultipleSuspectEventsSameWindowDifferentTimes() throws Exception {
        JChannel[] channels = createChannels();
        try {
            CaptureProtocol capture = new CaptureProtocol(1, Event.SUSPECT);
            ProtocolStack stack = channels[0].getProtocolStack();
            stack.insertProtocol(capture, ProtocolStack.Position.ABOVE, VERIFY_SUSPECT2.class);

            Address[] addrs=new Address[channels.length];
            for(int i=0; i < channels.length; i++)
                addrs[i]=channels[i].getAddress();

            // Now close B,C,D,E (this won't result in view changes as we have no failure detection protocol):
            for(int i=1; i < channels.length; i++)
                Util.shutdown(channels[i]);

            TP transport=channels[0].getProtocolStack().getTransport();
            // Inject SUSPECT(B,C) and SUSPECT(D,E) in 2 separate threads, spaced apart by a few ms:
            // we ensure different windows
            Suspecter s1=new Suspecter(List.of(addrs[1], addrs[2]), 0, transport),
                    s2=new Suspecter(List.of(addrs[3], addrs[4]), 100, transport);
            long start_time=System.currentTimeMillis();
            s1.start();
            s2.start();
            Util.waitUntil(VIEW_ACK_COLLECTION_TIMEOUT*2, 100, () -> channels[0].getView().size() == 1);
            long time=System.currentTimeMillis()-start_time;
            System.out.printf("%s: view=%s (took %d ms)\n", channels[0].getAddress(), channels[0].getView(), time);
            assert capture.count() == 1 : String.format("should have received %d suspect events but got %d", 1, capture.count());
            assert time < VIEW_ACK_COLLECTION_TIMEOUT: String.format("took %d ms, but should have taken less than %d",
                                                                     time, VIEW_ACK_COLLECTION_TIMEOUT);
        }
        finally {
            for(JChannel ch: channels)
                Util.shutdown(ch);
        }
    }

    /**
     * in this case we send two different all suspect events at the same time
     * so it should land in one window and therefore one event
     */
    public void testMultipleSuspectEventsSameWindowAtOnce() throws Exception {
        JChannel[] channels = createChannels();
        try {
            CaptureProtocol capture = new CaptureProtocol(1, Event.SUSPECT);
            ProtocolStack stack = channels[0].getProtocolStack();
            stack.insertProtocol(capture, ProtocolStack.Position.ABOVE, VERIFY_SUSPECT2.class);

            Address[] addrs=new Address[channels.length];
            for(int i=0; i < channels.length; i++)
                addrs[i]=channels[i].getAddress();

            // Now close B,C,D,E (this won't result in view changes as we have no failure detection protocol):
            for(int i=1; i < channels.length; i++)
                Util.shutdown(channels[i]);

            TP transport=channels[0].getProtocolStack().getTransport();
            // we ensure same window
            Suspecter s1=new Suspecter(List.of(addrs[1], addrs[2], addrs[3], addrs[4]), 0, transport);
            long start_time=System.currentTimeMillis();
            s1.start();
            Util.waitUntil(VIEW_ACK_COLLECTION_TIMEOUT*2, 100, () -> channels[0].getView().size() == 1);
            long time=System.currentTimeMillis()-start_time;
            System.out.printf("%s: view=%s (took %d ms)\n", channels[0].getAddress(), channels[0].getView(), time);
            assert capture.count() == 1;
            assert time < VIEW_ACK_COLLECTION_TIMEOUT: String.format("took %d ms, but should have taken less than %d",
                                                                     time, VIEW_ACK_COLLECTION_TIMEOUT);
        }
        finally {
            Util.close(channels[0]);
        }
    }

    static class CaptureProtocol extends Protocol {
        CountDownLatch latch;
        int eventType;
        final AtomicInteger count=new AtomicInteger();

        public CaptureProtocol(int numberOfEvents, int eventType) {
            this.latch = new CountDownLatch(numberOfEvents);
            this.eventType = eventType;
        }

        public int count() {
            return count.get();
        }

        @Override
        public Object up(Event evt) {
            if (evt.type() == eventType) {
                count.incrementAndGet();
                System.out.printf("suspect(%s): count=%d\n", evt.arg(), count.get());
                latch.countDown();
            }
            return super.up(evt);
        }

        public boolean await(int timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
    }

    private static JChannel[] createChannels() throws Exception {
        JChannel[] channels=new JChannel[5];

        for(int i=0; i < channels.length; i++) {
            channels[i]=new JChannel(Util.getTestStack()).name(Character.toString(('A' + i)));
            GMS gms=channels[i].getProtocolStack().findProtocol(GMS.class);
            gms.printLocalAddress(false).logViewWarnings(false);
            if(i == 0) { // only add VERIFY_SUSPECT2 to A
                ProtocolStack stack=channels[i].getProtocolStack();
                VERIFY_SUSPECT2 ver=new VERIFY_SUSPECT2().setTimeout(1000);
                stack.insertProtocol(ver, ProtocolStack.Position.ABOVE, Discovery.class);
                ver.init();
                ver.start();
                gms.setViewAckCollectionTimeout(VIEW_ACK_COLLECTION_TIMEOUT);
            }
            channels[i].connect(VERIFY_SUSPECT_Test.class.getSimpleName());
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        System.out.printf("-- Channels: %s\n", Stream.of(channels).map(JChannel::getAddress).collect(Collectors.toList()));

        channels[0].setReceiver(new Receiver() {
            public void viewAccepted(View new_view) {
                System.out.printf("** view: %s\n", new_view);
            }
        });
        return channels;
    }
    
    protected static class Suspecter extends Thread {
        protected final Collection<Address> suspected_mbrs;
        protected final long                sleep_time;
        protected final Protocol            prot;

        public Suspecter(Collection<Address> suspected_mbrs, long sleep_time, Protocol p) {
            this.suspected_mbrs=new ArrayList<>(suspected_mbrs);
            this.sleep_time=sleep_time;
            this.prot=p;
        }

        public void run() {
            Util.sleep(sleep_time);
            System.out.printf("%s: injecting SUSPECT(%s)\n", Thread.currentThread(), suspected_mbrs);
            prot.up(new Event(Event.SUSPECT, suspected_mbrs));
        }

        @Override
        public String toString() {
            return String.format("Suspecter%s", suspected_mbrs);
        }
    }

    protected class ProtImpl extends Protocol {
        protected final Map<Address,Long> map=new HashMap<>();

        public Map<Address,Long> getMap() {return map;}

        public Object up(Event evt) {
            switch(evt.getType()) {
                case Event.SUSPECT:
                    Collection<Address> suspects=evt.getArg();
                    long diff=System.currentTimeMillis() - start;
                    for(Address suspect: suspects) {
                        map.put(suspect, diff);
                        System.out.println("[" + diff + "] evt = " + evt);
                    }
                  break;
                case Event.UNSUSPECT:
                    Address mbr=evt.getArg();
                    map.remove(mbr);
                    break;
            }
            return null;
        }
    }

    protected static class NoopProtocol extends Protocol {
        public Object down(Event evt) {return null;}
        public Object down(Message msg) {return null;}
        public ThreadFactory getThreadFactory() {return new DefaultThreadFactory("y", false, true);}
    }
}
