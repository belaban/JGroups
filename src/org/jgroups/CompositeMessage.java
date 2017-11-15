
package org.jgroups;


import org.jgroups.util.ByteArray;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A message composed of multiple messages. This is useful when multiple messages are to be passed
 * in a single message. Example: a byte buffer (1000 bytes) with a request type (req/rsp/ack).
 * <br/>
 * In versions prior to 5.0, the byte arrays had to be copied into a single, larger (1001 bytes), byte array in
 * order to be passed to the message.
 * <br/>
 * @author Bela Ban
 * @since  5.0
 */
public class CompositeMessage extends BaseMessage {
    protected Message[]                   msgs;
    protected int                         index; // index of the next message to be added
    protected static final MessageFactory mf=new DefaultMessageFactory();

    public CompositeMessage() {
        super(true);
    }

    public CompositeMessage(boolean create_headers) {
        super(create_headers);
    }

    public CompositeMessage(Address dest) {
        super(dest);
    }

    public CompositeMessage(Address dest, Message ... messages) {
        super(dest);
        add(messages);
    }


    public Supplier<Message>      create()                              {return CompositeMessage::new;}
    public byte                   getType()                             {return Message.COMPOSITE_MSG;}
    public boolean                hasPayload()                          {return msgs != null && index > 0;}
    public boolean                hasArray()                            {return false;}
    public int                    getNumberOfMessages()                 {return index;}
    public int                    getOffset()                           {throw new UnsupportedOperationException();}
    public byte[]                 getArray()                            {throw new UnsupportedOperationException();}
    public CompositeMessage       setArray(byte[] b, int off, int len)  {throw new UnsupportedOperationException();}
    public CompositeMessage       setArray(ByteArray buf)               {throw new UnsupportedOperationException();}
    public CompositeMessage       setObject(Object obj)                 {throw new UnsupportedOperationException();}
    public <T extends Object>  T  getObject()                           {throw new UnsupportedOperationException();}

    public int getLength() {
        int total=0;
        for(int i=0; i < index && msgs != null; i++)
            total+=msgs[i].getLength();
        return total;
    }


    /** Adds the message at the end of the array. Increases the array if needed */
    public <T extends CompositeMessage> T add(Message msg) {
        ensureSameDest(msg);
        ensureCapacity(index);
        msgs[index++]=Objects.requireNonNull(msg);
        return (T)this;
    }

    public <T extends CompositeMessage> T add(Message ... messages) {
        ensureCapacity(index + messages.length);
        for(Message msg: messages)
            msgs[index++]=Objects.requireNonNull(ensureSameDest(msg));
        return (T)this;
    }


    /** Adds the message at the head of the array. Increases the array if needed and shifts
     *  messages behind the new message to the right by one */
    public <T extends CompositeMessage> T addAtHead(Message msg) {
        Objects.requireNonNull(msg);
        ensureCapacity(index);
        System.arraycopy(msgs, 0, msgs, 1, index++);
        msgs[0]=msg;
        return (T)this;
    }

    public <T extends Message> T get(int index) {
        return (T)msgs[index];
    }

    /** Removes a message at the end */
    public <T extends Message> T remove() {
        if(index == 0 || msgs == null)
            return null;
        T retval=(T)msgs[--index];
        msgs[index]=null;
        return retval;
    }

    /** Removes a message at the head */
    public <T extends Message> T removeAtHead() {
        if(index == 0 || msgs == null)
            return null;
        T retval=(T)msgs[0];
        System.arraycopy(msgs, 1, msgs, 0, index-1);
        msgs[--index]=null;
        return retval;
    }


    /**
     * Create a copy of this {@link CompositeMessage}.<br/>
     * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
     * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
     * modify the headers ! If you want to change a header, copy it and call
     * {@link CompositeMessage#putHeader(short,Header)} again.
    */
    public CompositeMessage copy(boolean copy_payload, boolean copy_headers) {
        CompositeMessage retval=new CompositeMessage(false);
        retval.dest=dest;
        retval.sender=sender;
        short tmp_flags=this.flags;
        byte tmp_tflags=this.transient_flags;
        retval.flags=tmp_flags;
        retval.transient_flags=tmp_tflags;
        retval.headers=copy_headers && headers != null? Headers.copy(this.headers) : createHeaders(Util.DEFAULT_HEADERS);
        if(copy_payload && msgs != null) {
            Message[] copy=new Message[msgs.length];
            for(int i=0; i < msgs.length; i++) {
                if(msgs[i] != null)
                    copy[i]=msgs[i].copy(copy_payload, copy_headers);
            }
            retval.msgs=copy;
            retval.index=index;
        }
        return retval;
    }


    public String toString() {
        return String.format("%s, %d message(s)", super.toString(), getNumberOfMessages());
    }

    public int size() {
        int retval=super.size() + Global.INT_SIZE; // length
        if(msgs != null) {
            for(int i=0; i < index; i++)
                retval+=msgs[i].size() + Global.BYTE_SIZE; // type
        }
        return retval;
    }


    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        writePayload(out);
    }


   /**
    * Writes the message to the output stream, but excludes the dest and src addresses unless the
    * src address given as argument is different from the message's src address
    * @param excluded_headers Don't marshal headers that are part of excluded_headers
    */
    @Override public void writeToNoAddrs(Address src, DataOutput out, short... excluded_headers) throws IOException {
        super.writeToNoAddrs(src, out, excluded_headers);
        writePayload(out);
    }


    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        readPayload(in);
    }

    protected void writePayload(DataOutput out) throws IOException {
        out.writeInt(index);
        if(msgs != null) {
            for(int i=0; i < index; i++) {
                Message msg=msgs[i];
                out.writeByte(msg.getType());
                msg.writeTo(out);
            }
        }
    }

    protected void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        index=in.readInt();
        if(index > 0) {
            msgs=new Message[index]; // a bit of additional space should we add byte arrays
            for(int i=0; i < index; i++) {
                byte type=in.readByte();
                msgs[i]=mf.create(type);
                msgs[i].readFrom(in);
            }
        }
    }

    /* --------------------------------- End of Interface Streamable ----------------------------- */

    protected void ensureCapacity(int size) {
        if(msgs == null)
            msgs=new Message[size+1];
        else if(size >= msgs.length)
            msgs=Arrays.copyOf(msgs, size+1);
    }

    protected Message ensureSameDest(Message msg) {
        if(!Objects.equals(dest, msg.dest()))
            throw new IllegalStateException(String.format("message's destination (%s) does not match destination of CompositeMessage (%s)",
                                                          msg.dest(), dest));
        return msg;
    }


}
