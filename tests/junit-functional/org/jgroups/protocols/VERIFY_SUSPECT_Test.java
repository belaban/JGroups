package org.jgroups.protocols;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * Tests https://issues.redhat.com/browse/JGRP-1429
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.TIME_SENSITIVE,singleThreaded=true)
public class VERIFY_SUSPECT_Test {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");
    protected static final long VIEW_ACK_COLLECTION_TIMEOUT=10000;
    protected long       start;


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
     * so it should land in one window and therefore one event
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
            s1.setName("suspecter-1");
            s2.setName("suspecter-2");
            verify.stop();
            verify.start(); // we ensure we start from T0 = 0s so the s1 get in the first window and s2 in the second
            s1.start();
            s2.start();

            assert capture.await(5, TimeUnit.SECONDS) : "we needed two events"; // you need to check when the view is 1
        }
        finally {
            Util.close(channels);
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
                    s2=new Suspecter(List.of(addrs[3], addrs[4]), 500, transport);
            s1.setName("suspecter-1");
            s2.setName("suspecter-2");
            long start_time=System.currentTimeMillis();
            s1.start();
            s2.start();
            Util.waitUntil(VIEW_ACK_COLLECTION_TIMEOUT*2, 100, () -> channels[0].getView().size() == 1);
            long time=System.currentTimeMillis()-start_time;
            System.out.printf("%s: view=%s (took %d ms)\n", channels[0].getAddress(), channels[0].getView(), time);
            assert capture.count == 1;
            assert time < VIEW_ACK_COLLECTION_TIMEOUT: String.format("took %d ms, but should have taken less than %d",
                                                                     time, VIEW_ACK_COLLECTION_TIMEOUT);
        }
        finally {
            Util.close(channels);
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
            s1.setName("suspecter-1");
            long start_time=System.currentTimeMillis();
            s1.start();
            Util.waitUntil(VIEW_ACK_COLLECTION_TIMEOUT*2, 100, () -> channels[0].getView().size() == 1);
            long time=System.currentTimeMillis()-start_time;
            System.out.printf("%s: view=%s (took %d ms)\n", channels[0].getAddress(), channels[0].getView(), time);
            assert capture.count == 1;
            assert time < VIEW_ACK_COLLECTION_TIMEOUT: String.format("took %d ms, but should have taken less than %d",
                                                                     time, VIEW_ACK_COLLECTION_TIMEOUT);
        }
        finally {
            Util.close(channels);
        }
    }

    class CaptureProtocol extends Protocol {
        CountDownLatch latch;
        int eventType;
        volatile int count;

        public CaptureProtocol(int numberOfEvents, int eventType) {
            this.latch = new CountDownLatch(numberOfEvents);
            this.eventType = eventType;
        }

        @Override
        public Object up(Event evt) {
            if (evt.type() == eventType) {
                latch.countDown();
                count++;
            }
            return super.up(evt);
        }

        @Override
        public Object down(Event evt) {
            return super.down(evt);
        }

        public boolean await(int timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
    }

    private JChannel[] createChannels() throws Exception {
        JChannel[] channels=new JChannel[5];

        for(int i=0; i < channels.length; i++) {
            channels[i]=new JChannel(Util.getTestStack()).name(Character.toString(('A' + i)));
            GMS gms=channels[i].getProtocolStack().findProtocol(GMS.class);
            gms.printLocalAddress(false);
            if(i == 0) { // only add VERIFY_SUSPECT2 to A
                ProtocolStack stack=channels[i].getProtocolStack();
                VERIFY_SUSPECT2 ver=new VERIFY_SUSPECT2().setTimeout(1000);
                stack.insertProtocol(ver, ProtocolStack.Position.ABOVE, Discovery.class);
                ver.init();
                ver.start();
                gms=stack.findProtocol(GMS.class);
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
            System.out.printf("%s: done\n", Thread.currentThread());
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
