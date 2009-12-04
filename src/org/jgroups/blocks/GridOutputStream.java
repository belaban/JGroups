package org.jgroups.blocks;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.*;

/**
 * @author Bela Ban
 * @version $Id: GridOutputStream.java,v 1.3 2009/12/04 16:48:38 belaban Exp $
 */
public class GridOutputStream extends OutputStream {
    final ReplCache<String,byte[]> cache;
    final short                    repl_count;
    final int                      chunk_size;
    final String                   name;

    int                            index=0;                // index into the file for writing
    int                            local_index=0;
    final byte[]                   current_buffer;
    static final Log               log=LogFactory.getLog(GridOutputStream.class);
    

    public GridOutputStream(String name, ReplCache<String,byte[]> cache, short repl_count, int chunk_size) throws FileNotFoundException {
        this(name, false, cache, repl_count, chunk_size);
    }

    public GridOutputStream(String name, boolean append, ReplCache<String,byte[]> cache, short repl_count, int chunk_size) throws FileNotFoundException {
        this.name=name;
        this.cache=cache;
        this.repl_count=repl_count;
        this.chunk_size=chunk_size;
        current_buffer=new byte[chunk_size];
    }



    public void write(int b) throws IOException {
        int remaining=getBytesRemainingInChunk();
        if(remaining == 0) {
            flush();
            local_index=0;
            remaining=chunk_size;
        }
        current_buffer[local_index]=(byte)b;
        local_index++;
        index++;
    }


    public void write(byte[] b) throws IOException {
        if(b != null)
            write(b, 0, b.length);
    }



    public void write(byte[] b, int off, int len) throws IOException {
        while(len > 0) {
            int remaining=getBytesRemainingInChunk();
            if(remaining == 0) {
                flush();
                local_index=0;
                remaining=chunk_size;
            }
            int bytes_to_write=Math.min(remaining, len);
            System.arraycopy(b, off, current_buffer, local_index, bytes_to_write);
            off+=bytes_to_write;
            len-=bytes_to_write;
            local_index+=bytes_to_write;
            index+=bytes_to_write;
        }
    }


    public void close() throws IOException {
        flush();
        reset();
    }

    public void flush() throws IOException {
        int chunk_number=getChunkNumber();
        String key=name + "#" + chunk_number;
        byte[] val=new byte[local_index];
        System.arraycopy(current_buffer, 0, val, 0, local_index);
        cache.put(key, val, repl_count, 0);
        if(log.isTraceEnabled())
            log.trace("put(): index=" + index + ", key=" + key + ": " + val.length + " bytes");
    }

    private int getBytesRemainingInChunk() {
        return chunk_size - local_index;
    }



    private int getChunkNumber() {
        return (index-1) / chunk_size;
    }

    private void reset() {
        index=local_index=0;
    }


 
}
