package org.jgroups.nio;

import java.nio.ByteBuffer;

/**
 * An impl of {@link Receiver}. Will get removed with the switch to Java 8; instead we'll use a default impl in Receiver
 * @author Bela Ban
 * @since  3.6.5
 */
public class ReceiverAdapter<A> implements Receiver<A> {
    public void receive(A sender, byte[] buf, int offset, int length) {

    }

    /**
     * The default implementation assumes that {@link ByteBuffer#flip()}  or {@link ByteBuffer#rewind()} was called on
     * buf before invoking this callback
     * @param sender
     * @param buf
     */
    public void receive(A sender, ByteBuffer buf) {
        if(buf == null)
            return;
        int offset=buf.hasArray()? buf.arrayOffset() : 0,
          len=buf.remaining();
        if(!buf.isDirect())
            receive(sender, buf.array(), offset, len);
        else { // by default use a copy; but of course implementers of Receiver can override this
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, len);
            receive(sender, tmp, 0, len);
        }
    }
}
