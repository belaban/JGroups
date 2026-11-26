package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.NioClient;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests transfer of messages from client to server
 * @author Bela Ban
 * @since  3.6.7
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class NioServerTest {
    protected static final int     NUM_MSGS=10000;
    protected static final int     NUM_SENDERS=25;
    protected static final int     MSG_SIZE=1000;
    protected static final int     recv_buf_size=50000, send_buf_size=10000;
    protected NioServer            srv;
    protected NioClient            client;
    protected final AtomicInteger  counter=new AtomicInteger(0);
    protected final Sender[]       senders=new Sender[NUM_SENDERS];
    protected final CountDownLatch latch=new CountDownLatch(1);

    @BeforeMethod protected void init() throws Exception {
        srv=new NioServer(Util.getLoopback(), 0);
        srv.sendBufferSize(send_buf_size).receiveBufferSize(recv_buf_size);
        srv.start();
        client=new NioClient(null, 0, Util.getLoopback(), ((IpAddress)srv.localAddress()).getPort());
        client.sendBufferSize(send_buf_size).receiveBufferSize(recv_buf_size);
        client.maxSendBuffers(1000);
        client.start();
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(counter, latch, client);
            senders[i].start();
        }
    }

    @AfterMethod protected void destroy() {Util.close(client, srv);}

    public void testTransfer() throws Exception {
        MyReceiver receiver=new MyReceiver();
        srv.receiver(receiver);

        latch.countDown(); // releases senders

        for(int i=0; i < 10; i++) {
            if(receiver.good() + receiver.bad() >= NUM_MSGS)
                break;
            Util.sleep(500);
        }

        System.out.printf("%d good buffers, %d bad buffers\n", receiver.good(), receiver.bad());

        if(receiver.bad() > 0) {
            List<byte[]> bad_msgs=receiver.badMsgs();
            for(byte[] arr: bad_msgs)
                System.out.printf("bad buffer: length=%d\n", arr.length);
            assert receiver.bad() == 0 : String.format("%d bad msgs, %d good msgs\n", receiver.bad(), receiver.good());
        }
    }

    protected static class Sender extends Thread {
        protected final AtomicInteger  cnt;
        protected final CountDownLatch latch;
        protected final NioClient      client;

        public Sender(AtomicInteger cnt, CountDownLatch latch, NioClient client) {
            this.cnt=cnt;
            this.latch=latch;
            this.client=client;
        }

        public void run() {
            int num_sends=0;
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            while(cnt.incrementAndGet() <= NUM_MSGS) {
                num_sends++;
                try {
                    client.send(new byte[MSG_SIZE], 0, MSG_SIZE);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.printf("Thread %s sent %d msgs\n", Thread.currentThread().getId(), num_sends);
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected int good, bad;
        protected List<byte[]> bad_msgs=new ArrayList<>(1000);

        public int          good()    {return good;}
        public int          bad()     {return bad;}
        public List<byte[]> badMsgs() {return bad_msgs;}

        public void receive(Address sender, byte[] buf, int offset, int length) {
            if(length != MSG_SIZE) {
                bad++;
                byte[] copy=new byte[length];
                System.arraycopy(buf, offset, copy, 0, length);
                bad_msgs.add(copy);
            }
            else
                good++;
        }
    }
}
