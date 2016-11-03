package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Compresses the payload of a message. Goal is to reduce the number of messages
 * sent across the wire. Should ideally be layered somewhere above a
 * fragmentation protocol (e.g. FRAG).
 * 
 * @author Bela Ban
 */
@MBean(description="Compresses messages to send and uncompresses received messages")
public class COMPRESS extends Protocol {   

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Compression level (from java.util.zip.Deflater) " +
      "(0=no compression, 1=best speed, 9=best compression). Default is 9")
    protected int compression_level=Deflater.BEST_COMPRESSION; // this is 9
   
    @Property(description="Minimal payload size of a message (in bytes) for compression to kick in. Default is 500 bytes")
    protected long min_size=500;
    
    @Property(description="Number of inflaters/deflaters for concurrent processing. Default is 2 ")
    protected int pool_size=2;
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    
    protected BlockingQueue<Deflater> deflater_pool=null;
    protected BlockingQueue<Inflater> inflater_pool=null;

    

    public COMPRESS() {      
    }


    public void init() throws Exception {
        deflater_pool=new ArrayBlockingQueue<>(pool_size);
        for(int i=0; i < pool_size; i++)
            deflater_pool.add(new Deflater(compression_level));
        inflater_pool=new ArrayBlockingQueue<>(pool_size);
        for(int i=0; i < pool_size; i++)
            inflater_pool.add(new Inflater());
    }

    public void destroy() {
        deflater_pool.forEach(Deflater::end);
        inflater_pool.forEach(Inflater::end);
    }   


    /**
     * We compress the payload if it is larger than {@code min_size}. In this case we add a header containing
     * the original size before compression. Otherwise we add no header.<p>
     * Note that we compress either the entire buffer (if offset/length are not used), or a subset (if offset/length
     * are used)
     */
    public Object down(Message msg) {
        int length=msg.getLength(); // takes offset/length (if set) into account
        if(length >= min_size) {
            byte[] payload=msg.getRawBuffer(); // here we get the ref so we can avoid copying
            byte[] compressed_payload=new byte[length];
            Deflater deflater=null;
            try {
                deflater=deflater_pool.take();
                deflater.reset();
                deflater.setInput(payload, msg.getOffset(), length);
                deflater.finish();
                deflater.deflate(compressed_payload);
                int compressed_size=deflater.getTotalOut();

                if(compressed_size < length ) { // JGRP-1000
                    Message copy=msg.copy(false).putHeader(this.id,new CompressHeader(length))
                      .setBuffer(compressed_payload, 0, compressed_size);
                    if(log.isTraceEnabled())
                        log.trace("compressed payload from %d bytes to %d bytes", length, compressed_size);
                    return down_prot.down(copy);
                }
                else {
                    if(log.isTraceEnabled())
                        log.trace("skipping compression since the compressed message (%d) is not " +
                                    "smaller than the original (%d)", compressed_size, length);
                }
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt(); // set interrupt flag again
                throw new RuntimeException(e);
            }
            finally {
                if(deflater != null)
                    deflater_pool.offer(deflater);
            }
        }
        return down_prot.down(msg);
    }

    /**
     * If there is no header, we pass the message up. Otherwise we uncompress the payload to its original size.
     */
    public Object up(Message msg) {
        CompressHeader hdr=msg.getHeader(this.id);
        if(hdr != null) {
            Message uncompressed_msg=uncompress(msg, hdr.original_size);
            if(uncompressed_msg != null) {
                if(log.isTraceEnabled())
                    log.trace("uncompressed %d bytes to %d bytes", msg.getLength(), uncompressed_msg.getLength());
                return up_prot.up(uncompressed_msg);
            }
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            CompressHeader hdr=msg.getHeader(this.id);
            if(hdr != null) {
                Message uncompressed_msg=uncompress(msg, hdr.original_size);
                if(uncompressed_msg != null) {
                    if(log.isTraceEnabled())
                        log.trace("uncompressed %d bytes to %d bytes", msg.getLength(), uncompressed_msg.getLength());
                    batch.replace(msg, uncompressed_msg); // replace msg in batch with uncompressed_msg
                }
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /** Returns a new message as a result of uncompressing msg, or null if msg couldn't be uncompressed */
    protected Message uncompress(Message msg, int original_size) {
        byte[] compressed_payload=msg.getRawBuffer();
        if(compressed_payload != null && compressed_payload.length > 0) {
            byte[] uncompressed_payload=new byte[original_size];
            Inflater inflater=null;
            try {
                inflater=inflater_pool.take();
                inflater.reset();
                inflater.setInput(compressed_payload, msg.getOffset(), msg.getLength());
                try {
                    inflater.inflate(uncompressed_payload);
                    // we need to copy: https://jira.jboss.org/jira/browse/JGRP-867
                    return msg.copy(false).setBuffer(uncompressed_payload);
                }
                catch(DataFormatException e) {
                    log.error(Util.getMessage("CompressionFailure"), e);
                }
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt(); // set the interrupt bit again, so caller can handle it
            }
            finally {
                if(inflater != null)
                    inflater_pool.offer(inflater);
            }
        }
        return null;
    }



    public static class CompressHeader extends Header {
        int original_size=0;

        public CompressHeader() {
            super();
        }

        public CompressHeader(int s) {
            original_size=s;
        }

        public short getMagicId() {return 58;}

        public Supplier<? extends Header> create() {
            return CompressHeader::new;
        }

        public int serializedSize() {
            return Global.INT_SIZE;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeInt(original_size);
        }

        public void readFrom(DataInput in) throws Exception {
            original_size=in.readInt();
        }
    }
}
