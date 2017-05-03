package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

/**
 * NIO based client for measuring heap-based vs direct byte buffers. Use {@link NioServerPerfTest} as server
 * @author Bela Ban
 * @since  3.6.4
 */
public class NioClientTest {
    protected volatile boolean    running=true;
    protected final LongAdder     total_bytes_sent=new LongAdder();
    protected final LongAdder     total_msgs=new LongAdder();
    protected Sender[]            senders;



    protected static ByteBuffer create(int size, boolean direct) {
        return direct? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    protected void start(InetAddress host, boolean direct, int num_threads) throws Exception {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress("[1] send [x] exit");
            switch(c) {
                case '1':
                    sendMessages(host, direct, num_threads);
                    break;
                case 'x':
                case -1:
                    looping=false;
                    break;
            }
        }
    }


    protected void sendMessages(InetAddress host, boolean direct, int num_threads) throws Exception {
        total_msgs.reset();
        total_bytes_sent.reset();
        senders=new Sender[num_threads];
        final CountDownLatch latch=new CountDownLatch(1);
        for(int i=0; i < senders.length; i++)
            senders[i]=new Sender(host, direct, latch);
        for(Sender sender: senders)
            sender.start();
        latch.countDown();
        for(Sender sender: senders)
            sender.join();
    }



    protected class Sender extends Thread {
        protected SocketChannel        ch;
        protected final CountDownLatch latch;
        protected final InetAddress    host;
        protected final boolean        direct;
        protected final ByteBuffer     buf;

        public Sender(InetAddress host, boolean direct, CountDownLatch latch) {
            this.latch=latch;
            this.host=host;
            this.direct=direct;
            buf=create(NioServerPerfTest.SIZE, direct);
        }

        public void run() {
             try {
                 ch=SocketChannel.open();
                 ch.configureBlocking(true); // we want blocking behavior
                 ch.connect(new InetSocketAddress(host, 7500));
                 latch.await();
             }
             catch(Exception e) {
                e.printStackTrace();
            }
            for(;;) {
                total_bytes_sent.add(NioServerPerfTest.SIZE);
                if(total_bytes_sent.sum() > NioServerPerfTest.BYTES_TO_SEND)
                    break;
                buf.rewind();
                try {
                    ch.write(buf);
                    total_msgs.increment();
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }

            Util.close(ch);
        }
    }


    public static void main(String[] args) throws Exception {
        String host="localhost";
        boolean direct=false;
        int num_threads=5;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-host")) {
                host=args[++i];
                continue;
            }
            if(args[i].equals("-direct")) {
                direct=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("NioClientTest [-host host] [-direct true|false] [-num_threads num]");
            return;
        }

        new NioClientTest().start(InetAddress.getByName(host), direct, num_threads);
    }


}
