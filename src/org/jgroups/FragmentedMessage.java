package org.jgroups;

import org.jgroups.util.PartialOutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * A message which refers to another message, but only marshals ({@link org.jgroups.util.Streamable#writeTo(DataOutput)})
 * a part of the original message, starting at a given {@link #offset} and marshalling only {@link #length} bytes.
 * <br/>
 * The marshalling is done using {@link org.jgroups.util.PartialOutputStream}.<br/>
 * Used by {@link org.jgroups.protocols.FRAG4}.
 *
 * @author Bela Ban
 * @since  5.0
 */
public class FragmentedMessage extends BytesMessage { // we need the superclass' byte array for de-serialization only
    protected Message original_msg;

    public FragmentedMessage() { // for de-serialization
    }

    public FragmentedMessage(Message original_msg, int off, int len) {
        this.original_msg=original_msg;
        this.offset=off;
        this.length=len;
    }

    public Message getOriginalMessage() {
        return original_msg;
    }

    public short getType() {return Message.FRAG_MSG;}

    public Supplier<Message> create() {
        return FragmentedMessage::new;
    }

    protected int sizeOfPayload() {
        return Global.INT_SIZE + length;
    }

    protected void writePayload(DataOutput out) throws IOException {
        out.writeInt(length);
        PartialOutputStream pos=new PartialOutputStream(out, offset, length);
        original_msg.writeTo(pos);
    }

    protected void readPayload(DataInput in) throws IOException {
        this.length=in.readInt();
        if(this.length > 0) {
            this.array=new byte[this.length];
            in.readFully(this.array);
        }
    }


    public String toString() {
        return String.format("%s [off=%d len=%d] (original msg: %s)",
                             FragmentedMessage.class.getSimpleName(), offset, length, original_msg);
    }


    protected <T extends BytesMessage> T createMessage() {
        return (T)new FragmentedMessage();
    }
}
