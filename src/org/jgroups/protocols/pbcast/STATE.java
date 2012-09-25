package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.util.BlockingInputStream;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * STATE streams the state (written to an OutputStream) to the state requester in chunks (defined by
 * chunk_size). Every chunk is sent via a unicast message. The state requester writes the chunks into a blocking
 * input stream ({@link BlockingInputStream}) from which the {@link MessageListener#setState(java.io.InputStream)}
 * reads it. The size of the BlockingInputStream is buffer_size bytes.
 * <p/>
 * When implementing {@link MessageListener#getState(java.io.OutputStream)}, the state should be written in sizeable
 * chunks, because the underlying output stream generates 1 message / write. So if there are 1000 writes of 1 byte
 * each, this would generate 1000 messages ! We suggest using a {@link java.io.BufferedOutputStream} over the output
 * stream handed to the application as argument of the callback.
 * <p/>
 * When implementing the {@link MessageListener#setState(java.io.InputStream)} callback, there is no need to use a
 * {@link java.io.BufferedOutputStream}, as the input stream handed to the application already buffers incoming data
 * internally.
 * @author Bela Ban
 * @author Vladimir Blagojevic
 * @since 2.4
 */
@MBean(description="Streaming state transfer protocol")
public class STATE extends StreamingStateTransfer {


    /*
    * --------------------------------------------- Fields ---------------------------------------
    */

    /** If use_default_transport is true, we consume bytes off of this blocking queue. Used on the state
     * <em>requester</em> side only */
    protected volatile BlockingInputStream input_stream=null;




    public STATE() {
        super();
    }



    protected void handleViewChange(View v) {
        super.handleViewChange(v);
        if(state_provider != null && !v.getMembers().contains(state_provider)) {
            Util.close(input_stream);
            openBarrierAndResumeStable();
            Exception ex=new EOFException("state provider " + state_provider + " left");
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult(ex)));
        }
    }

    protected void handleEOF(Address sender) {
        Util.close(input_stream);
        super.handleEOF(sender);
    }

    protected void handleException(Throwable exception) {
        Util.close(input_stream);
        super.handleException(exception);
    }

    protected void handleStateChunk(Address sender, byte[] buffer, int offset, int length) {
        if(buffer == null || input_stream == null)
            return;
        try {
            if(log.isDebugEnabled())
                log.debug(local_addr + " received state chunk of " + Util.printBytes(length) + " from " + sender);
            input_stream.write(buffer, offset, length);
        }
        catch(IOException e) {
            handleException(e);
        }
    }



    protected void createStreamToRequester(Address requester) {
        OutputStream bos=new StateOutputStream(requester);
        getStateFromApplication(requester, bos, true);
    }

    
    protected void createStreamToProvider(final Address provider, final StateHeader hdr) {
        Util.close(input_stream);
        input_stream=new BlockingInputStream(buffer_size);

        // use another thread to read state because the state requester has to receive state chunks from the state provider
        Thread t=getThreadFactory().newThread(new Runnable() {
            public void run() {
                setStateInApplication(provider, input_stream, hdr.getDigest());
            }
        }, "STATE state reader");
        t.start();
    }




    protected class StateOutputStream extends OutputStream {
        protected final Address stateRequester;
        protected final AtomicBoolean closed;
        protected long bytesWrittenCounter=0;

        public StateOutputStream(Address stateRequester) {
            this.stateRequester=stateRequester;
            this.closed=new AtomicBoolean(false);
        }

        public void close() throws IOException {
            if(closed.compareAndSet(false, true)) {
                if(stats)
                    avg_state_size=num_bytes_sent.addAndGet(bytesWrittenCounter) / num_state_reqs.doubleValue();
            }
        }

        public void write(byte[] b, int off, int len) throws IOException {
            if(closed.get())
                throw new IOException("The output stream is closed");
            sendMessage(b, off, len);
        }

        public void write(byte[] b) throws IOException {
            if(closed.get())
                throw new IOException("The output stream is closed");
            sendMessage(b, 0, b.length);
        }

        public void write(int b) throws IOException {
            if(closed.get())
                throw new IOException("The output stream is closed");
            byte buf[]={(byte)b};
            write(buf);
        }


        protected void sendMessage(byte[] b, int off, int len) throws IOException {
            Message m=new Message(stateRequester);
            m.putHeader(id, new StateHeader(StateHeader.STATE_PART));
            m.setBuffer(b, off, len);
            bytesWrittenCounter+=len;
            if(Thread.interrupted())
                throw interrupted((int)bytesWrittenCounter);
            down_prot.down(new Event(Event.MSG, m));
            if(log.isDebugEnabled())
                log.debug(local_addr + " sent " + Util.printBytes(len) + " of state to " + stateRequester);
        }


        protected InterruptedIOException interrupted(int cnt) {
            final InterruptedIOException ex=new InterruptedIOException();
            ex.bytesTransferred=cnt;
            return ex;
        }
    }

}
