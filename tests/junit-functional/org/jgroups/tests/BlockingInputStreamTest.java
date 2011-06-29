package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.BlockingInputStream;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Tests {@link org.jgroups.util.BlockingInputStream}
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class BlockingInputStreamTest {

    
    public void testCreation() throws IOException {
        BlockingInputStream in=new BlockingInputStream(2000);
        System.out.println("in = " + in);
        assert in.available() == 0 && in.capacity() == 2000;

        in.write(new byte[]{'b', 'e', 'l', 'a'});
        System.out.println("in = " + in);
        assert in.available() == 4 && in.capacity() == 2000;
    }


    public void testBlockingReadAndClose() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        final CountDownLatch latch=new CountDownLatch(1);

        new Thread() {
            public void run() {
                try {
                    latch.await();
                    Util.sleep(1000);
                    in.close();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        byte[] buf=new byte[100];
        latch.countDown();
        int num=in.read(buf, 0, buf.length);
        System.out.println("num = " + num);
        assert num == -1 : " expected -1 (EOF) but got " + num;
    }


    public void testReadOnClosedInputStream() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        in.close();
        byte[] buf=new byte[100];
        int num=in.read(buf, 0, buf.length);
        System.out.println("num = " + num);
        assert num == -1 : " expected -1 (EOF) but got " + num;
    }

    public void testSimpleTransfer() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(100);
        new Thread() {
            public void run() {
                byte[] buffer=new byte[500];
                for(int i=0; i < buffer.length; i++) {
                    if(i % 2 == 0)
                        buffer[i]=0;
                    else
                        buffer[i]=1;
                }
                try {
                    in.write(buffer, 0, buffer.length);
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
                finally {
                    Util.close(in);
                }
            }
        }.start();

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
        final BlockingInputStream in=new BlockingInputStream(2048);
        final byte[] buffer=generateBuffer(100000);

        new Thread() {
            public void run() {
                try {
                    in.write(buffer, 0, buffer.length);
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
                finally {
                    Util.close(in);
                }
            }
        }.start();

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


    protected byte[] generateBuffer(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++)
            buf[i]=(byte)(Util.random(size) % Byte.MAX_VALUE);
        return buf;
    }
    
    public void testBlockingWriteAndClose() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(3);
        final CountDownLatch latch=new CountDownLatch(1);

        new Thread() {
            public void run() {
                try {
                    latch.await();
                    Util.sleep(1000);
                    in.close();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        byte[] buf=new byte[]{'B', 'e', 'l', 'a'};
        latch.countDown();
        in.write(buf, 0, buf.length);
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

        new Thread() {
            public void run() {
                try {
                    for(int i=1; i <= 10; i++) {
                        byte[] buf=("Hello world " + i).getBytes();
                        try {
                            in.write(buf);
                        }
                        catch(IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                finally {
                    Util.close(in);
                }
            }
        }.start();

        Util.sleep(500);
        
        int size=in.available();
        byte[] buf=new byte[200];
        int num=in.read(buf);
        assert num == size;
    }


    public void testWriteExceedingCapacity() throws IOException {
        final BlockingInputStream in=new BlockingInputStream(10);
        new Thread() {
            public void run() {
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
            }
        }.start();

        byte[] buffer=new byte[15];
        try {
            in.write(buffer);
        }
        finally {
            Util.close(in);
        }
    }
}
