package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.protocols.Bundler;
import org.jgroups.protocols.RingBufferBundlerLockless;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test
public class RingBundlerTestLockless {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D");

    public void testSimpleSend() throws Exception {
        RingBufferBundlerLockless bundler=new RingBufferBundlerLockless(16);
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
        RingBufferBundlerLockless bundler=new RingBufferBundlerLockless(16);
        RingBundlerTest.MockTransport transport=new RingBundlerTest.MockTransport();
        bundler.init(transport);

        for(int i =0; i < 6; i++)
            bundler.send(new EmptyMessage(null));

        int cnt=bundler.size();
        assert cnt == 6;

        bundler._readMessages();
        System.out.println("bundler = " + bundler);

        assert bundler.readIndex() == 6;
        assert bundler.writeIndex() == 6;
        assert bundler.size() == 0;
        assert transport.map.get(null) == 1;
        transport.map.clear();

        for(Message msg: create(10000, null, a, a, a, b, c, d, d, a, null, null, a))
            bundler.send(msg);
        System.out.println("bundler = " + bundler);

        cnt=bundler.size();
        assert cnt == 12;
        assert bundler.readIndex() == 6;
        assert bundler.writeIndex() == 2;

        bundler._readMessages();

        assert bundler.readIndex() == 2;
        assert bundler.writeIndex() == 2;
        assert bundler.size() == 0;
        Stream.of(null, a, b, c, d).forEach(msg -> {assert transport.map.get(msg) == 1;});
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

}
