package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.Bundler;
import org.jgroups.protocols.RingBufferBundlerLockless2;
import org.jgroups.protocols.TP;
import org.jgroups.util.AsciiString;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test
public class RingBundlerTestLockless2 {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D");

    public void testSimpleSend() throws Exception {
        RingBufferBundlerLockless2 bundler=new RingBufferBundlerLockless2(16);
        RingBundlerTest.MockTransport transport=new RingBundlerTest.MockTransport();
        bundler.init(transport);

        final CountDownLatch latch=new CountDownLatch(1);
        Sender[] senders=new Sender[20];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(latch, bundler);
            senders[i].start();
        }

        latch.countDown();
        for(Sender sender: senders)
            sender.join();

        System.out.println("bundler = " + bundler);

        bundler._readMessages();

        System.out.println("bundler = " + bundler);
    }

    public void testSendToMultipleDestinations() throws Exception {
        RingBufferBundlerLockless2 bundler=new RingBufferBundlerLockless2(16);
        RingBundlerTest.MockTransport transport=new RingBundlerTest.MockTransport();
        bundler.init(transport);

        for(int i =0; i < 6; i++)
            bundler.send(new EmptyMessage(null));

        int cnt=bundler.size();
        assert cnt == 6;

        bundler._readMessages();
        System.out.println("bundler = " + bundler);

        assert bundler.readIndex() == 6;
        assert bundler.writeIndex() == 7;
        assert bundler.size() == 0;
        assert transport.map.get(null) == 1;
        transport.map.clear();

        for(Message msg: create(10000, null, a, a, a, b, c, d, d, a, null, null, a))
            bundler.send(msg);
        System.out.println("bundler = " + bundler);

        cnt=bundler.size();
        assert cnt == 12;
        assert bundler.readIndex() == 6;
        assert bundler.writeIndex() == 3;

        bundler._readMessages();

        assert bundler.readIndex() == 2;
        assert bundler.writeIndex() == 3;
        assert bundler.size() == 0;
        Stream.of(null, a, b, c, d).forEach(msg -> {assert transport.map.get(msg) == 1;});
    }

    public void testSendWithNULL_MSG() throws Exception {
        RingBufferBundlerLockless2 bundler=new RingBufferBundlerLockless2(16);
        RingBundlerTest.MockTransport transport=new RingBundlerTest.MockTransport();
        bundler.init(transport);

        Message[] buf=(Message[])Util.getField(Util.getField(RingBufferBundlerLockless2.class, "buf"), bundler);
        Field write_index_field=Util.getField(RingBufferBundlerLockless2.class, "write_index");
        write_index_field.setAccessible(true);
        buf[1]=buf[2]=RingBufferBundlerLockless2.NULL_MSG;
        buf[3]=null;
        write_index_field.set(bundler, 4);
        System.out.println("bundler = " + bundler);
        assert bundler.size() == 3;
        bundler._readMessages();
        System.out.println("bundler = " + bundler);
        assert bundler.writeIndex() == 4;
        assert bundler.readIndex() == 2;
        assert bundler.size() == 1;
        buf[3]=new EmptyMessage(null);
        bundler._readMessages();
        assert bundler.readIndex() == 3;
        assert bundler.size() == 0;
    }


    protected List<Message> create(int msg_size, Address ... destinations) {
        List<Message> list=new ArrayList<>(destinations.length);
        for(Address dest: destinations)
            list.add(new BytesMessage(dest, new byte[msg_size]));
        return list;
    }


    protected static class Sender extends Thread {
        protected final CountDownLatch latch;
        protected final Bundler        bundler;

        public Sender(CountDownLatch latch, Bundler bundler) {
            this.latch=latch;
            this.bundler=bundler;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            try {
                bundler.send(new EmptyMessage(a));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }







    protected static class MockTransport extends TP {
        protected final Map<Address,Integer> map=new HashMap<>();

        public MockTransport() {
            this.cluster_name=new AsciiString("mock");
            thread_factory=new DefaultThreadFactory("", false);
        }

        public boolean supportsMulticasting() {
            return false;
        }


        public void sendMulticast(byte[] data, int offset, int length) throws Exception {
            incrCount(null);
        }

        protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
            incrCount(dest);
        }

        public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {

        }

        public String getInfo() {
            return null;
        }

        protected PhysicalAddress getPhysicalAddress() {
            return null;
        }

        protected void incrCount(Address dest) {
            Integer count=map.get(dest);
            if(count == null)
                map.put(dest, 1);
            else
                map.put(dest, count+1);
        }
    }
}
