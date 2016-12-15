package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.util.BlockingInputStream;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.*;
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
     * <em>requester</em> side only. Note that we cannot use a PipedInputStream as we have multiple writer threads
     * pushing data into the input stream */
    protected volatile BlockingInputStream input_stream;




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
            if(log.isTraceEnabled())
                log.trace("%s: received chunk of %s from %s",local_addr,Util.printBytes(length),sender);
            input_stream.write(buffer, offset, length);
        }
        catch(IOException e) {
            handleException(e);
        }
    }



    protected void createStreamToRequester(Address requester) {
        OutputStream bos=new StateOutputStream(requester);
        getStateFromApplication(requester, bos, false);
    }

    protected Tuple<InputStream,Object> createStreamToProvider(final Address provider, final StateHeader hdr) {
        Util.close(input_stream);
        input_stream=new BlockingInputStream(buffer_size);
        return new Tuple<>(input_stream, null);
    }

    @Override
    protected boolean useAsyncStateDelivery() {return true;}

    protected class StateOutputStream extends OutputStream {
        protected final Address        stateRequester;
        protected final AtomicBoolean  closed;
        protected long                 bytesWrittenCounter;

        public StateOutputStream(Address stateRequester) {
            this.stateRequester=stateRequester;
            this.closed=new AtomicBoolean(false);
        }

        public void close() throws IOException {
            if(closed.compareAndSet(false, true) && stats) {
                num_bytes_sent.add(bytesWrittenCounter);
                avg_state_size=num_bytes_sent.sum() / num_state_reqs.doubleValue();
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
            Message m=new Message(stateRequester).putHeader(id, new StateHeader(StateHeader.STATE_PART));

            // we're copying the buffer passed from the state provider here: if a BufferedOutputStream is used, the
            // buffer (b) will always be the same and can be modified after it has been set in the message !

            // Fix for https://issues.jboss.org/browse/JGRP-1598
            byte[] data=new byte[len];
            System.arraycopy(b, off, data, 0, len);
            // m.setBuffer(b, off, len);
            m.setBuffer(data);

            bytesWrittenCounter+=len;
            if(Thread.interrupted())
                throw interrupted((int)bytesWrittenCounter);
            down_prot.down(m);
            if(log.isTraceEnabled())
                log.trace("%s: sent chunk of %s to %s",local_addr,Util.printBytes(len),stateRequester);
        }


        protected InterruptedIOException interrupted(int cnt) {
            final InterruptedIOException ex=new InterruptedIOException();
            ex.bytesTransferred=cnt;
            return ex;
        }
    }

}
