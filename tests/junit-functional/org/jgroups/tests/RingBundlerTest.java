package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.RingBufferBundler;
import org.jgroups.protocols.TP;
import org.jgroups.util.AsciiString;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.RingBuffer;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test(singleThreaded=true)
public class RingBundlerTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D");

    public void testReceiveAndSend() throws Exception {
        RingBufferBundler bundler=new RingBufferBundler(16);
        RingBuffer<Message> rb=bundler.buf();
        MockTransport transport=new MockTransport();
        bundler.init(transport);

        for(int i =0; i < 6; i++)
            bundler.send(new EmptyMessage(null));
        System.out.println("rb = " + rb);
        int cnt=rb.countLockLockless();
        assert cnt == 6;
        bundler.sendBundledMessages(rb.buf(), rb.readIndexLockless(), cnt);
        rb.publishReadIndex(cnt);
        System.out.println("rb = " + rb);
        assert rb.readIndex() == 6;
        assert rb.writeIndex() == 6;
        assert rb.isEmpty();
        assert transport.map.get(null) == 1;
        transport.map.clear();

        for(Message msg: create(10000, null, a, a, a, b, c, d, d, a, null, null, a))
            bundler.send(msg);
        System.out.println("rb = " + rb);
        cnt=rb.countLockLockless();
        assert cnt == 12;
        assert rb.readIndex() == 6;
        assert rb.writeIndex() == 2;

        bundler.sendBundledMessages(rb.buf(), rb.readIndexLockless(), rb.countLockLockless());
        rb.publishReadIndex(cnt);

        assert rb.readIndex() == 2;
        assert rb.writeIndex() == 2;
        assert rb.isEmpty();
        Stream.of(null, a,b,c,d).forEach(a -> {assert transport.map.get(a) == 1;});
    }

    public void testFullBufferAndRead() throws Exception {
        RingBufferBundler bundler=new RingBufferBundler(16);
        RingBuffer<Message> rb=bundler.buf();
        MockTransport transport=new MockTransport();
        bundler.init(transport);
        bundler.stop(); // stops the reader thread

        for(int i=0; i < 16; i++)
            bundler.send(new EmptyMessage(a)); // buffer is full now; reader is not running
        System.out.println("rb = " + rb);

        Thread[] adders=new Thread[16];
        for(int i=0; i < 16; i++) {
            adders[i]=new Thread(() -> {
                try {
                    bundler.send(new EmptyMessage(b));
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }, "Adder-" + i);
            adders[i].start();
        }

        int available=rb.waitForMessages(5, (it,spins) -> LockSupport.park());
        System.out.println("available = " + available);
        assert available == 16;
        // we skip the sending
        rb.publishReadIndex(available);

        while(!rb.isEmpty() || !allDone(adders)) {
            available=rb.waitForMessages(5, (it,spins) -> LockSupport.parkNanos(1));
            System.out.println("available = " + available);
            rb.publishReadIndex(available);
        }
        assert rb.isEmpty();
        assert allDone(adders);
    }

    protected static boolean allDone(Thread[] threads) {
        for(Thread thread: threads)
            if(thread.isAlive())
                return false;
        return true;
    }

    protected List<Message> create(int msg_size, Address ... destinations) {
        List<Message> list=new ArrayList<>(destinations.length);
        for(Address dest: destinations)
            list.add(new BytesMessage(dest, new byte[msg_size]));
        return list;
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
            map.merge(dest, 1, (a1, b1) -> a1 + b1);
        }
    }
}
