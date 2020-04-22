package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.BlockingInputStream;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Tests {@link org.jgroups.util.BlockingInputStream}
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class BlockingInputStreamTest {

    
    public void testCreation() throws IOException {
        BlockingInputStream in=new BlockingInputStream(2000);
        System.out.println("in = " + in);
        assert in.available() == 0 && in.capacity() == 2000;

        in.write(new byte[]{'b', 'e', 'l', 'a'});
        System.out.println("in = " + in);
        assert in.available() == 4 && in.capacity() == 2000;
    }


    public void testRead() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        byte[] input={'B', 'e', 'l', 'a'};
        in.write(input);
        in.close();

        assert in.available() == 4;
        for(int i=0; i < input.length; i++) {
            int b=in.read();
            assert b == input[i];
        }
        int b=in.read();
        assert b == -1;
    }


    public void testBlockingReadAndClose() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        final CountDownLatch latch=new CountDownLatch(1);
        byte[] buf=new byte[100];
        
        new Closer(latch, in, 1000L).start(); // closes input stream after 1 sec
        latch.countDown();
        int num=in.read(buf, 0, buf.length);
        assert num == -1 : " expected -1 (EOF) but got " + num;
    }

    
    public void testBlockingWriteAndClose() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(3);
        final CountDownLatch latch=new CountDownLatch(1);
        byte[] buf={'B', 'e', 'l', 'a'};

        new Closer(latch, in, 1000L).start(); // closes input stream after 1 sec
        latch.countDown();
        in.write(buf, 0, buf.length);
    }


    public void testReadOnClosedInputStream() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        in.close();
        byte[] buf=new byte[100];
        int num=in.read(buf, 0, buf.length);
        assert num == -1 : " expected -1 (EOF) but got " + num;
    }

    
    public void testWriteCloseRead() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        for(int i=1; i <= 5; i++) {
            byte[] buf=("Hello world " + i).getBytes();
            in.write(buf);
        }
        in.close();

        int size=in.available();
        byte[] buf=new byte[100];
        int num=in.read(buf);
        assert num == size;
    }


    public void testWriteCloseRead2() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        StringBuilder sb=new StringBuilder();
        for(int i=1; i <=10; i++)
            sb.append("Hello world " + i);
        byte[] buffer=sb.toString().getBytes();
        new Writer(in, buffer).start();
        Util.sleep(500);

        byte[] buf=new byte[200];
        int num=in.read(buf);
        assert num == buffer.length;
    }

    public void testWriteCloseRead3() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(300);
        StringBuilder sb=new StringBuilder();
        for(int i=1; i <=10; i++)
            sb.append("Hello world " + i);
        byte[] buffer=sb.toString().getBytes();
        new Writer(in, buffer).execute(); // don't use a separate thread

        byte[] buf=new byte[200];
        int num=in.read(buf);
        assert num == buffer.length;
    }


    
    public void testSimpleTransfer() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        byte[] buffer=new byte[500];
        for(int i=0; i < buffer.length; i++)
            buffer[i]=(byte)(i % 2 == 0? 0 : 1);
        new Writer(in, buffer).start();
        
        byte[] tmp=new byte[500];
        int offset=0;
        while(true) {
            int bytes=in.read(tmp, offset, tmp.length - offset);
            if(bytes == -1)
                break;
            offset+=bytes;
        }
        System.out.println("read " + offset + " bytes");
        assert offset == 500 : "offset is " + offset + " but expected 500";
        for(int i=0; i < tmp.length; i++) {
            if(i % 2 == 0)
                assert tmp[i] == 0;
            else
                assert tmp[i] == 1;
        }
    }


    public void testLargeTransfer() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(8192);
        final byte[] buffer=generateBuffer(1000000);
        new Writer(in, buffer).start();

        byte[] tmp=new byte[buffer.length];
        int offset=0;
        while(true) {
            int bytes=in.read(tmp, offset, tmp.length - offset);
            if(bytes == -1)
                break;
            offset+=bytes;
        }
        System.out.println("read " + offset + " bytes");
        assert offset == buffer.length : "offset is " + offset + " but expected " + buffer.length;
        System.out.print("Verifying that the buffers are the same: ");
        for(int i=0; i < tmp.length; i++)
            assert buffer[i] == tmp[i];
        System.out.println("OK");
    }

    public void testLargeTransfer2() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(8192);
        final byte[] buffer=generateBuffer(1000000);
        new Writer(in, buffer).start();

        byte[] tmp=new byte[buffer.length];
        int bytes=in.read(tmp); // reads 1 million bytes in one go

        System.out.println("read " + bytes + " bytes");
        assert bytes == buffer.length : "read " + bytes + " bytes but expected " + buffer.length;
        System.out.print("Verifying that the buffers are the same: ");
        for(int i=0; i < tmp.length; i++)
            assert buffer[i] == tmp[i];
        System.out.println("OK");
    }

    public void testWriterMultipleChunks() throws Exception {
        final BlockingInputStream in=new BlockingInputStream(100);
        final byte[] buffer=generateBuffer(500);
        Writer writer=new Writer(in, buffer, 5, true);
        writer.start();

        byte[] tmp=new byte[20];
        int num=0;
        while(true) {
            int read=in.read(tmp);
            if(read == -1)
                break;
            num+=read;
        }
        System.out.println("read " + num + " bytes");
        assert num == 5 * buffer.length;
    }

    public void testMultipleWriters() throws Exception {
        final BlockingInputStream in=new BlockingInputStream(100);
        final byte[] buffer=generateBuffer(500);

        final Writer[] writers=new Writer[5];
        for(int i=0; i < writers.length; i++) {
            writers[i]=new Writer(in, buffer, 1, false);
            writers[i].setName("writer-" + (i+1));
            writers[i].start();
        }

        new Thread(() -> {
            while(true) {
                boolean all_done=true;
                for(Writer writer: writers) {
                    if(writer.isAlive()) {
                        all_done=false;
                        break;
                    }
                }
                if(all_done) {
                    Util.close(in);
                    return;
                }
                else
                    Util.sleep(100);
            }
        }).start();

        byte[] tmp=new byte[400];
        int num=0;
        while(true) {
            int read=in.read(tmp, 0, tmp.length);
            if(read == -1)
                break;
            num+=read;
        }
        System.out.println("read " + num + " bytes");
        assert num == writers.length * buffer.length;

        for(Writer writer: writers)
            assert writer.isAlive() == false;
    }


    public void testWriteExceedingCapacity() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(10);
        new Thread(() -> {
            byte[] tmp=new byte[20];
            int num=0;
            try {
                while(true) {
                    int read=in.read(tmp);
                    if(read == -1)
                        break;
                    num+=read;
                }
                System.out.println("read " + num + " bytes");
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }).start();

        byte[] buffer=new byte[15];
        try {
            in.write(buffer);
        }
        finally {
            Util.close(in);
        }
    }


    public void testWritingBeyondLength() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(800);

        new Thread(() -> {
            byte[] buf=new byte[800+600];
            try {
                in.write(buf);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }).start();

        byte[] buf=new byte[1000];
        int read=in.read(buf);
        assert read == buf.length;
    }


    public void testSimpleWrite() throws Exception {
        final BlockingInputStream input=new BlockingInputStream(8192);
        byte[] in={'B', 'e', 'l', 'a'};
        input.write(in);

        byte[] buf=new byte[5];
        for(int i=0; i < in.length; i++) {
            int read=input.read(buf, i, 1);
            assert read == 1;
        }
        for(int i=0; i < in.length; i++)
            assert in[i] == buf[i];
    }


    public void testObjectStreaming() throws Exception {
        final BlockingInputStream input=new BlockingInputStream(8192);

        Map<String,List<Long>> map=new HashMap<>(4);
        for(String key: Arrays.asList("A", "B", "C", "D")) {
            List<Long> list=new ArrayList<>(1000);
            map.put(key, list);
            for(int i=1; i <= 1000; i++)
                list.add((long)i);
        }

        ByteArrayOutputStream output=new ByteArrayOutputStream(8192);
        OutputStream out=new BufferedOutputStream(output);
        Util.objectToStream(map, new DataOutputStream(out));
        out.flush();
        final byte[] buffer=output.toByteArray();

        Thread writer=new Thread(() -> {
            try {
                input.write(buffer);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        });
        writer.start();

        Map<String,List<Long>> tmp=Util.objectFromStream(new DataInputStream(input));
        assert tmp.size() == 4;
        for(String key: Arrays.asList("A", "B", "C", "D")) {
            List<Long> list=map.get(key);
            assert list.size() == 1000;
            assert list.iterator().next() == 1;
        }
    }


    protected static byte[] generateBuffer(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++)
            buf[i]=(byte)(Util.random(size) % Byte.MAX_VALUE);
        return buf;
    }


    protected static final class Closer extends Thread {
        protected final CountDownLatch latch;
        protected final InputStream in;
        protected final long timeout;

        public Closer(CountDownLatch latch, InputStream in, long timeout) {
            this.latch=latch;
            this.in=in;
            this.timeout=timeout;
        }

        public void run() {
            try {
                latch.await();
                Util.sleep(timeout);
                in.close();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected static final class Writer extends Thread {
        protected final BlockingInputStream in;
        protected final byte[]              buffer;
        protected final int                 num_times;
        protected final boolean             close_input;

        public Writer(BlockingInputStream in, byte[] buffer, int num_times, boolean close_input) {
            this.in=in;
            this.buffer=buffer;
            this.num_times=num_times;
            this.close_input=close_input;
        }

        public Writer(BlockingInputStream in, byte[] buffer) {
            this(in, buffer, 1, true);
        }

        public void run() {
           execute();
        }

        public void execute() {
            try {
                for(int i=0; i < num_times; i++) {
                    in.write(buffer, 0, buffer.length);
                    System.out.println(Thread.currentThread().getId() + ": wrote " + buffer.length + " bytes");
                }
                if(close_input)
                    Util.close(in);
            }
            catch(IOException e) {
                System.err.println(e);
            }
        }
    }


}
