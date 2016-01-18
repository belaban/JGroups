package org.jgroups.blocks;

import org.jgroups.annotations.Experimental;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Bela Ban
 */
@Experimental
public class GridInputStream extends InputStream {
    final ReplCache<String,byte[]> cache;
    final int                      chunk_size;
    final String                   name;
    protected final GridFile       file; // file representing this input stream
    int                            index=0;                // index into the file for writing
    int                            local_index=0;
    byte[]                         current_buffer=null;
    boolean                        end_reached=false;
    final static Log               log=LogFactory.getLog(GridInputStream.class);



    GridInputStream(GridFile file, ReplCache<String, byte[]> cache, int chunk_size) throws FileNotFoundException {
        this.file=file;
        this.name=file.getPath();
        this.cache=cache;
        this.chunk_size=chunk_size;
    }



    public int read() throws IOException {
        int bytes_remaining_to_read=getBytesRemainingInChunk();
        if(bytes_remaining_to_read == 0) {
            if(end_reached)
                return -1;
            current_buffer=fetchNextChunk();
            local_index=0;
            if(current_buffer == null)
                return -1;
            else if(current_buffer.length < chunk_size)
                end_reached=true;
            bytes_remaining_to_read=getBytesRemainingInChunk();
        }
        int retval=current_buffer[local_index++];
        index++;
        return retval;
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        int bytes_read=0;
        while(len > 0) {
            int bytes_remaining_to_read=getBytesRemainingInChunk();
            if(bytes_remaining_to_read == 0) {
                if(end_reached)
                    return bytes_read > 0? bytes_read : -1;
                current_buffer=fetchNextChunk();
                local_index=0;
                if(current_buffer == null)
                    return bytes_read > 0? bytes_read : -1;
                else if(current_buffer.length < chunk_size)
                    end_reached=true;
                bytes_remaining_to_read=getBytesRemainingInChunk();
            }
            int bytes_to_read=Math.min(len, bytes_remaining_to_read);
            // bytes_to_read=Math.min(bytes_to_read, current_buffer.length - local_index);
            System.arraycopy(current_buffer, local_index, b, off, bytes_to_read);
            local_index+=bytes_to_read;
            off+=bytes_to_read;
            len-=bytes_to_read;
            bytes_read+=bytes_to_read;
            index+=bytes_to_read;
        }

        return bytes_read;
    }

    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException();
    }

    public int available() throws IOException {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
        local_index=index=0;
        end_reached=false;
    }

    private int getBytesRemainingInChunk() {
        // return chunk_size - local_index;
        return current_buffer == null? 0 : current_buffer.length - local_index;
    }

    private byte[] fetchNextChunk() {
        int chunk_number=getChunkNumber();
        String key=name + ".#" + chunk_number;
        byte[] val= cache.get(key);
        if(log.isTraceEnabled())
            log.trace("fetching index=" + index + ", key=" + key +": " + (val != null? val.length + " bytes" : "null"));
        return val;
    }

    private int getChunkNumber() {
        return index / chunk_size;
    }

}
