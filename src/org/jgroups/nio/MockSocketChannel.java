package org.jgroups.nio;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;

/**
 * A mock {@link java.nio.channels.SocketChannel} for testing
 * @author Bela Ban
 * @since  3.6.5
 */
public class MockSocketChannel extends SocketChannel {
    protected int        bytes_to_write;
    protected ByteBuffer bytes_to_read;
    protected boolean    closed=false;
    protected ByteBuffer recorder; // records writes if set


    public MockSocketChannel() {
        super(null);
    }

    /**
     * Initializes a new instance of this class.
     * @param provider The provider that created this channel
     */
    public MockSocketChannel(SelectorProvider provider) {
        super(provider);
    }


    public MockSocketChannel bytesToWrite(int num) {
        bytes_to_write=num; return this;
    }

    public MockSocketChannel bytesToRead(byte[] buf) {
        bytes_to_read=ByteBuffer.wrap(buf);
        return this;
    }

    public MockSocketChannel bytesToRead(ByteBuffer buf) {
        bytes_to_read=buf;
        return this;
    }

    public ByteBuffer bytesToRead() {return bytes_to_read;}

    public MockSocketChannel recorder(ByteBuffer buf) {this.recorder=buf; return this;}
    public ByteBuffer        recorder()               {return recorder;}


    @Override
    public SocketChannel bind(SocketAddress local) throws IOException {
        return null;
    }

    public void doClose() {closed=true;}

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        return null;
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return null;
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        return null;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        return null;
    }

    @Override
    public Socket socket() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean isConnectionPending() {
        return false;
    }

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        return true;
    }

    @Override
    public boolean finishConnect() throws IOException {
        return true;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        return null;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read=0;
        while(dst.hasRemaining() && bytes_to_read.hasRemaining()) {
            dst.put(bytes_to_read.get());
            read++;
        }
        return read > 0? read : closed? -1 : read;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long total=0;
        for(int i=offset; i < offset+length; i++) {
            ByteBuffer buf=i >=0 && i < dsts.length? dsts[i] : null;
            if(buf != null) {
                int read=read(buf);
                if(read >= 0)
                    total+=read;
                else
                    return read;
            }
        }
        return total;
    }

    @Override
    public int write(ByteBuffer buf) throws IOException {
        if(bytes_to_write == 0)
            return 0;
        int written=0;
        while(buf.hasRemaining() && bytes_to_write-- > 0) {
            byte b=buf.get();
            written++;
            if(recorder != null)
                recorder.put(b);
        }
        return written;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if(bytes_to_write == 0)
            return 0;
        int written=0;
        for(int i=offset; i < Math.min(srcs.length, length+offset); i++) {
            ByteBuffer buf=srcs[i];
            while(buf.hasRemaining() && bytes_to_write-- > 0) {
                byte b=buf.get();
                written++;
                if(recorder != null)
                    recorder.put(b);
            }
        }
        return written;
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return null;
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {

    }

    @Override
    protected void implConfigureBlocking(boolean block) throws IOException {

    }
}
