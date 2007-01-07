package org.jgroups.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Maintains a pool of ExposedDataOutputStreams. The main reason is that a ByteArrayOutputStream starts with 1024
 * bytes, and almost always increases to 65K (max size of a UDP datagram). We save a few copies when the BAOS increases
 * its size by pooling those.
 * @author Bela Ban
 * @version $Id: OutgoingBufferPool.java,v 1.1 2007/01/07 01:24:52 belaban Exp $
 */
public class OutgoingBufferPool {
    private BlockingQueue<ExposedDataOutputStream> buffers;


    public OutgoingBufferPool(int capacity) {
        buffers=new ArrayBlockingQueue<ExposedDataOutputStream>(capacity);
        for(int i=0; i < capacity; i++) {
            ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(1024);
            ExposedDataOutputStream      dos=new ExposedDataOutputStream(out_stream);
            try {
                buffers.put(dos);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }


    public ExposedDataOutputStream take() throws InterruptedException {
        return buffers.take();
    }

    public void put(ExposedDataOutputStream buf) throws InterruptedException {
        ((ExposedByteArrayOutputStream)buf.getOutputStream()).reset();
        buf.reset();
        buffers.put(buf);
    }


    public String dumpStats() {
        ExposedByteArrayOutputStream stream;
        StringBuilder sb=new StringBuilder();
        sb.append(buffers.size()).append(" elements, capacities:\n");
        for(ExposedDataOutputStream buf: buffers) {
            stream=(ExposedByteArrayOutputStream)buf.getOutputStream();
            sb.append("size=").append(stream.size()).append(", capacity=").append(stream.getCapacity()).append(")\n");
        }
        return sb.toString();
    }


}
