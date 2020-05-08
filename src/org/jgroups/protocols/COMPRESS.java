package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.MessageIterator;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.LongAdder;
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
   
    @Property(description="Minimal payload size of a message (in bytes) for compression to kick in. Default is 500 bytes",
      type=AttributeType.BYTES)
    protected int min_size=500;
    
    @Property(description="Number of inflaters/deflaters for concurrent processing. Default is 2 ")
    protected int pool_size=2;
    
    protected BlockingQueue<Deflater> deflater_pool;
    protected BlockingQueue<Inflater> inflater_pool;
    protected MessageFactory          msg_factory;
    protected final LongAdder         num_compressions=new LongAdder(), num_decompressions=new LongAdder();




    public COMPRESS() {      
    }

    public int      getMinSize()      {return min_size;}
    public COMPRESS setMinSize(int s) {this.min_size=s; return this;}

    @ManagedAttribute(description="Number of compressions",type=AttributeType.SCALAR)
    public long getNumCompressions() {return num_compressions.sum();}

    @ManagedAttribute(description="Number of un-compressions",type=AttributeType.SCALAR)
    public long getNumUncompressions() {return num_decompressions.sum();}

    public void resetStats() {
        super.resetStats();
        num_compressions.reset(); num_decompressions.reset();
    }

    public void init() throws Exception {
        deflater_pool=new ArrayBlockingQueue<>(pool_size);
        for(int i=0; i < pool_size; i++)
            deflater_pool.add(new Deflater(compression_level));
        inflater_pool=new ArrayBlockingQueue<>(pool_size);
        for(int i=0; i < pool_size; i++)
            inflater_pool.add(new Inflater());
        msg_factory=getTransport().getMessageFactory();
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
            boolean serialize=!msg.hasArray();
            ByteArray tmp=null;
            byte[] payload=serialize? (tmp=messageToByteArray(msg)).getArray() : msg.getArray();
            int offset=serialize? tmp.getOffset() : msg.getOffset();
            length=serialize? tmp.getLength() : msg.getLength();
            byte[] compressed_payload=new byte[length];
            Deflater deflater=null;
            try {
                deflater=deflater_pool.take();
                deflater.reset();
                deflater.setInput(payload, offset, length);
                deflater.finish();
                deflater.deflate(compressed_payload);
                int compressed_size=deflater.getTotalOut();

                if(compressed_size < length ) { // JGRP-1000
                    Message copy=null;
                    if(serialize)
                        copy=new BytesMessage(msg.getDest());
                    else
                        copy=msg.copy(false, true);
                    copy.setArray(compressed_payload, 0, compressed_size)
                      .putHeader(this.id, new CompressHeader(length).needsDeserialization(serialize));
                    if(log.isTraceEnabled())
                        log.trace("compressed payload from %d bytes to %d bytes", length, compressed_size);
                    num_compressions.increment();
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
            Message uncompressed_msg=uncompress(msg, hdr.original_size, hdr.needsDeserialization());
            if(uncompressed_msg != null) {
                if(log.isTraceEnabled())
                    log.trace("uncompressed %d bytes to %d bytes", msg.getLength(), uncompressed_msg.getLength());
                num_decompressions.increment();
                return up_prot.up(uncompressed_msg);
            }
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        MessageIterator it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            CompressHeader hdr=msg.getHeader(this.id);
            if(hdr != null) {
                Message uncompressed_msg=uncompress(msg, hdr.original_size, hdr.needsDeserialization());
                if(uncompressed_msg != null) {
                    if(log.isTraceEnabled())
                        log.trace("uncompressed %d bytes to %d bytes", msg.getLength(), uncompressed_msg.getLength());
                    it.replace(uncompressed_msg); // replace msg in batch with uncompressed_msg
                    num_decompressions.increment();
                }
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /** Returns a new message as a result of uncompressing msg, or null if msg couldn't be uncompressed */
    protected Message uncompress(Message msg, int original_size, boolean needs_deserialization) {
        byte[] compressed_payload=msg.getArray();
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
                    if(needs_deserialization) {
                        return messageFromByteArray(uncompressed_payload, msg_factory);
                    }
                    else
                        return msg.copy(false, true).setArray(uncompressed_payload, 0, uncompressed_payload.length);
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

    protected static ByteArray messageToByteArray(Message msg) {
        try {
            return Util.messageToBuffer(msg);
        }
        catch(Exception ex) {
            throw new RuntimeException("failed marshalling message", ex);
        }
    }

    protected static Message messageFromByteArray(byte[] uncompressed_payload, MessageFactory msg_factory) {
        try {
            return Util.messageFromBuffer(uncompressed_payload, 0, uncompressed_payload.length, msg_factory);
        }
        catch(Exception ex) {
            throw new RuntimeException("failed unmarshalling message", ex);
        }
    }


    public static class CompressHeader extends Header {
        protected int     original_size;
        protected boolean needs_deserialization;

        public CompressHeader() {
            super();
        }

        public CompressHeader(int s) {
            original_size=s;
        }

        public short                      getMagicId()                       {return 58;}
        public Supplier<? extends Header> create()                           {return CompressHeader::new;}
        public boolean                    needsDeserialization()             {return needs_deserialization;}
        public CompressHeader             needsDeserialization(boolean flag) {needs_deserialization=flag; return this;}
        @Override public int              serializedSize()                   {return Global.INT_SIZE + Global.BYTE_SIZE;}

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(original_size);
            out.writeBoolean(needs_deserialization);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            original_size=in.readInt();
            needs_deserialization=in.readBoolean();
        }
    }
}
