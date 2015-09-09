package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NIO based server for measuring heap-based vs direct byte buffers. Use {@link NioClientTest} as client test driver
 * @author Bela Ban
 * @since  3.6.4
 */
public class NioServerPerfTest {
    protected ServerSocketChannel ch;
    protected Selector            selector;
    protected volatile boolean    running=true;
    protected ByteBuffer          buffer;
    protected final AtomicLong    total_bytes_received=new AtomicLong(0);
    protected final AtomicLong    total_msgs=new AtomicLong(0);
    protected final AtomicLong    start=new AtomicLong(0);

    public static final long      BYTES_TO_SEND=10_000_000;
    public static final int       SIZE=1000;


    protected static ByteBuffer create(int size, boolean direct) {
        return direct? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    protected void start(boolean direct) throws Exception {
        selector=Selector.open();

        ch=ServerSocketChannel.open();
        ch.bind(new InetSocketAddress("0.0.0.0", 7500));
        ch.configureBlocking(false);
        ch.register(selector, SelectionKey.OP_ACCEPT, null);
        System.out.println("-- server ready");

        while(running) {
            selector.select();
            Set<SelectionKey> keys=selector.selectedKeys();
            for(Iterator<SelectionKey> it=keys.iterator(); it.hasNext();) {
                SelectionKey key=it.next();
                if(!key.isValid()) {
                    it.remove();
                    continue;
                }
                it.remove();
                if(key.isAcceptable()) {
                    SocketChannel client_ch=ch.accept();
                    if(client_ch != null) { // accept() may return null...
                        System.out.printf("accepted connection from %s\n", client_ch.getRemoteAddress());
                        client_ch.configureBlocking(false);
                        client_ch.register(selector, SelectionKey.OP_READ, create(SIZE, direct));
                    }
                }
                else if(key.isReadable()) {
                    if(!handle((SocketChannel)key.channel(), (ByteBuffer)key.attachment())) {
                        key.cancel();
                        Util.close(key.channel());
                    }
                }
            }
        }

        Util.close(selector,ch);
    }


    protected boolean handle(SocketChannel ch, ByteBuffer buf) {
        try {
            if(start.get() == 0)
                start.compareAndSet(0, System.currentTimeMillis());
            int num=ch.read(buf);
            if(num < 0)
                return false;
            total_bytes_received.addAndGet(num);
            if(!buf.hasRemaining()) {
                total_msgs.incrementAndGet();
                buf.rewind();
            }
            if(total_bytes_received.get() >= BYTES_TO_SEND) {
                long time=System.currentTimeMillis() - start.get();
                double throughput_sec=total_bytes_received.get() / (time / 1000.0), msgs_sec=total_msgs.get() / (time / 1000.0);
                System.out.printf("\n===========================\nreceived %d messages in %.2f secs: throughput: %s/sec, %.2f msgs/sec\n",
                                  total_msgs.get(), time/1000.0, Util.printBytes(throughput_sec), msgs_sec);
                start.set(0);
                total_bytes_received.set(0);
                total_msgs.set(0);
                return false;
            }
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return true;
    }



    public static void main(String[] args) throws Exception {
        boolean direct=false;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-direct")) {
                direct=Boolean.parseBoolean(args[++i]);
                continue;
            }
            System.out.println("NioServerPerfTest [-direct true|false]");
            return;
        }

        new NioServerPerfTest().start(direct);
    }


}
