package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;

import java.io.*;
import java.util.Properties;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Compresses the payload of a message. Goal is to reduce the number of messages sent across the wire.
 * Should ideally be layered somewhere above a fragmentation protocol (e.g. FRAG).
 * @author Bela Ban
 * @version $Id: COMPRESS.java,v 1.12 2006/07/05 08:22:02 belaban Exp $
 */
public class COMPRESS extends Protocol {
    Deflater[] deflater_pool=null;
    Inflater[] inflater_pool=null;


    /** Values are from 0-9 (0=no compression, 9=best compression) */
    int compression_level=Deflater.BEST_COMPRESSION; // this is 9

    /** Minimal payload size of a message (in bytes) for compression to kick in */
    long min_size=500;

    /** Number of inflaters/deflaters, for concurrency, increase this to the max number of concurrent requests possible */
    int pool_size=2;

    private int inflater_index=0;
    private int deflater_index=0;

    final static String name="COMPRESS";

    public String getName() {
        return name;
    }


    private final int getInflaterIndex() {
        synchronized(this) {
            int retval=inflater_index++;
            if(inflater_index >= pool_size) {
                inflater_index=0;
            }
            return retval;
        }
    }


    private final int getDeflaterIndex() {
        synchronized(this) {
            int retval=deflater_index++;
            if(deflater_index >= pool_size) {
                deflater_index=0;
            }
            return retval;
        }
    }


    public void init() throws Exception {
        deflater_pool=new Deflater[pool_size];
        for(int i=0; i < deflater_pool.length; i++) {
            deflater_pool[i]=new Deflater(compression_level);
        }
        inflater_pool=new Inflater[pool_size];
        for(int i=0; i < inflater_pool.length; i++) {
            inflater_pool[i]=new Inflater();
        }
    }

    public void destroy() {
        for(int i=0; i < deflater_pool.length; i++) {
            Deflater deflater=deflater_pool[i];
            deflater.end();
        }
        for(int i=0; i < inflater_pool.length; i++) {
            Inflater inflater=inflater_pool[i];
            inflater.end();
        }
    }


    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("compression_level");
        if(str != null) {
            compression_level=Integer.parseInt(str);
            props.remove("compression_level");
        }

        str=props.getProperty("min_size");
        if(str != null) {
            min_size=Long.parseLong(str);
            props.remove("min_size");
        }

        str=props.getProperty("pool_size");
        if(str != null) {
            pool_size=Integer.parseInt(str);
            if(pool_size <= 0) {
                log.warn("pool_size must be > 0, setting it to 1");
                pool_size=1;
            }
            props.remove("pool_size");
        }

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }



    /**
     * We compress the payload if it is larger than <code>min_size</code>. In this case we add a header containing
     * the original size before compression. Otherwise we add no header.<br/>
     * Note that we compress either the entire buffer (if offset/length are not used), or a subset (if offset/length
     * are used)
     * @param evt
     */
    public void down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            int length=msg.getLength(); // takes offset/length (if set) into account
            if(length >= min_size) {
                byte[] payload=msg.getRawBuffer(); // here we get the ref so we can avoid copying
                byte[] compressed_payload=new byte[length];
                int compressed_size;
                int tmp_index=getDeflaterIndex();
                Deflater deflater=deflater_pool[tmp_index]; // must be guaranteed to be non-null !
                synchronized(deflater) {
                    deflater.reset();
                    deflater.setInput(payload, msg.getOffset(), length);
                    deflater.finish();
                    deflater.deflate(compressed_payload);
                    compressed_size=deflater.getTotalOut();
                }
                byte[] new_payload=new byte[compressed_size];
                System.arraycopy(compressed_payload, 0, new_payload, 0, compressed_size);
                msg.setBuffer(new_payload);
                msg.putHeader(name, new CompressHeader(length));
                if(trace)
                    log.trace("compressed payload from " + length + " bytes to " + compressed_size + " bytes (inflater #" +
                    tmp_index + ")");
            }
        }
        passDown(evt);
    }



    /**
     * If there is no header, we pass the message up. Otherwise we uncompress the payload to its original size.
     * @param evt
     */
    public void up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            CompressHeader hdr=(CompressHeader)msg.removeHeader(name);
            if(hdr != null) {
                byte[] compressed_payload=msg.getRawBuffer();
                if(compressed_payload != null && compressed_payload.length > 0) {
                    int original_size=hdr.original_size;
                    byte[] uncompressed_payload=new byte[original_size];
                    int tmp_index=getInflaterIndex();
                    Inflater inflater=inflater_pool[tmp_index];
                    synchronized(inflater) {
                        inflater.reset();
                        inflater.setInput(compressed_payload, msg.getOffset(), msg.getLength());
                        try {
                            inflater.inflate(uncompressed_payload);
                            if(trace)
                                log.trace("uncompressed " + compressed_payload.length + " bytes to " + original_size +
                                        " bytes (deflater #" + tmp_index + ")");
                            msg.setBuffer(uncompressed_payload);
                        }
                        catch(DataFormatException e) {
                            if(log.isErrorEnabled()) log.error("exception on uncompression", e);
                        }
                    }
                }
            }
        }
        passUp(evt);
    }






    public static class CompressHeader extends Header implements Streamable {
        int original_size=0;

        public CompressHeader() {
            super();
        }

        public CompressHeader(int s) {
            original_size=s;
        }


        public long size() {
            return Global.INT_SIZE;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(original_size);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            original_size=in.readInt();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(original_size);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            original_size=in.readInt();
        }
    }
}
