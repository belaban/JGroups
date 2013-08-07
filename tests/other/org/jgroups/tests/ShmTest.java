package org.jgroups.tests;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Tests the speed of message sending/receiving via shared memory. Start the server first, then the client.
 * The server and client use a simple (and stupid) alternating bit protocol to rendezvous; this could be enhanced into
 * a ring buffer to increase performance.
 * @author Bela Ban
 * @since  3.4
 */
public class ShmTest {
    protected static final int NUM=1000000;
    protected static final int SIZE=1000;
    protected static final int PRINT=NUM / 10;
    protected static final int MAX_BUSY_SPIN=1000;

    protected static void start(boolean server) throws Exception {
        RandomAccessFile file=new RandomAccessFile("/tmp/shm", "rwd");
        FileChannel channel=file.getChannel();
        MappedByteBuffer shared_buffer=channel.map(FileChannel.MapMode.READ_WRITE,0,SIZE + 10);
        file.close();
        int count=0; // messages written (server) or read (client)
        int busy_spin=0;

        if(server) {
            while(count < NUM) {
                byte input=shared_buffer.get(0);
                //System.out.println("input = " + input);
                if(input == 0) {
                    // write
                    byte[] buf=new byte[SIZE];
                    shared_buffer.put(buf, 0, buf.length);
                    shared_buffer.rewind();
                    shared_buffer.put((byte)1).rewind();
                    count++;
                    if(count % PRINT == 0)
                        System.out.println("wrote " + count);
                }
                else {
                    busy_spin=0;
                    while((input=shared_buffer.get(0)) != 0) {
                        if(busy_spin++ < MAX_BUSY_SPIN)
                            ;
                        else
                            Thread.yield();
                    }
                }
            }
        }
        else {
            long start=System.nanoTime();

            while(count < NUM) {
                byte input=shared_buffer.get(0);
                //System.out.println("input = " + input);
                if(input == 1) {
                    // write
                    byte[] buf=new byte[SIZE];
                    shared_buffer.get(buf,0,buf.length);
                    shared_buffer.rewind();
                    shared_buffer.put((byte)0).rewind();
                    count++;
                    if(count % PRINT == 0)
                        System.out.println("read " + count);
                }
                else {
                    busy_spin=0;
                    while((input=shared_buffer.get(0)) != 1) {
                        if(busy_spin++ < MAX_BUSY_SPIN)
                            ;
                        else
                            Thread.yield();
                    }
                }
            }


            long time=System.nanoTime() - start;
            long reads_sec=(long)(NUM / (time/1000.0/1000.0/1000.0));
            double ns_per_msg=time/NUM;
            double ms_per_msg=(time/1000000.0)/NUM;
            double throughput=(NUM * SIZE / 1000000) / (time/1000000000.0);
            System.out.println((time/1000000.0) + " ms, " + reads_sec + " reads/sec, " + ns_per_msg + " ns/msg, " +
                                 ms_per_msg + " ms/msg, " + throughput + " MB/sec");
        }

    }


    public static void main(String[] args) throws Exception {
        start(args.length > 0);
    }
}
