
package org.jgroups;


import org.jgroups.util.ByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A message composed of multiple messages. This is useful when multiple messages are to be passed
 * in a single message. Example: a byte buffer (1000 bytes) with a request type (req/rsp/ack).
 * <br/>
 * In versions prior to 5.0, the byte arrays had to be copied into a single, larger (1001 bytes), byte array in
 * order to be passed to the message.
 * <br/>
 * This class is unsynchronized; the envisaged use case is that a CompositeMessage is created with a number of messages,
 * or messages are added, but then the instance is not modified anymore and sent.
 * @author Bela Ban
 * @since  5.0
 */
public class CompositeMessage extends BaseMessage implements Iterable<Message> {
    protected Message[] msgs;
    protected int       index;    // index of the next message to be added
    protected boolean   collapse; // send as a BytesMessage when true


    protected static final MessageFactory mf=new DefaultMessageFactory();

    public CompositeMessage() {
    }


    public CompositeMessage(Address dest) {
        super(dest);
    }

    public CompositeMessage(Address dest, Message ... messages) {
        super(dest);
        add(messages);
    }


    public Supplier<Message>      create()                          {return CompositeMessage::new;}
    public short                  getType()                         {return collapse? Message.BYTES_MSG : Message.COMPOSITE_MSG;}
    public boolean                hasPayload()                      {return msgs != null && index > 0;}
    public boolean                hasArray()                        {return false;}
    public int                    getNumberOfMessages()             {return index;}
    public int                    getOffset()                       {throw new UnsupportedOperationException();}
    public byte[]                 getArray()                        {throw new UnsupportedOperationException();}
    public CompositeMessage       setArray(byte[] b, int o, int l)  {throw new UnsupportedOperationException();}
    public CompositeMessage       setArray(ByteArray buf)           {throw new UnsupportedOperationException();}
    public CompositeMessage       setObject(Object obj)             {throw new UnsupportedOperationException();}
    public <T extends Object>  T  getObject()                       {throw new UnsupportedOperationException();}
    public boolean                collapse()                        {return collapse;}
    public CompositeMessage       collapse(boolean b)               {collapse=b; return this;}



    public int getLength() {
        int total=0;
        for(int i=0; i < index && msgs != null; i++)
            total+=msgs[i].getLength();
        return total;
    }


    /** Adds the message at the end of the array. Increases the array if needed */
    public CompositeMessage add(Message msg) {
        ensureSameDest(msg);
        ensureCapacity(index);
        msgs[index++]=Objects.requireNonNull(msg);
        return this;
    }

    public CompositeMessage add(Message ... messages) {
        ensureCapacity(index + messages.length);
        for(Message msg: messages)
            msgs[index++]=Objects.requireNonNull(ensureSameDest(msg));
        return this;
    }


    public <T extends Message> T get(int index) {
        return (T)msgs[index];
    }


    /** Create a copy of this {@link CompositeMessage}. */
    public CompositeMessage copy(boolean copy_payload, boolean copy_headers) {
        CompositeMessage retval=(CompositeMessage)super.copy(copy_payload, copy_headers);
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
                retval+=msgs[i].size() + Global.SHORT_SIZE; // type
        }
        return retval;
    }

    public Iterator<Message> iterator() {
        return new CompositeMessageIterator();
    }


    public void writePayload(DataOutput out) throws IOException {
        if(collapse) {
            writePayloadAsBytes(out);
            return;
        }
        out.writeInt(index);
        if(msgs != null) {
            for(int i=0; i < index; i++) {
                Message msg=msgs[i];
                out.writeShort(msg.getType());
                msg.writeTo(out);
            }
        }
    }

    public void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        index=in.readInt();
        if(index > 0) {
            msgs=new Message[index]; // a bit of additional space should we add byte arrays
            for(int i=0; i < index; i++) {
                short type=in.readShort();
                msgs[i]=mf.create(type);
                msgs[i].readFrom(in);
            }
        }
    }

    // Writes all payloads (hasArray() == true) without any sizes in-between
    protected void writePayloadAsBytes(DataOutput out) throws IOException {
        int len=getLength();
        out.writeInt(msgs == null? -1 : len);
        if(msgs != null) {
            for(int i=0; i < index; i++) {
                Message msg=msgs[i];
                if(msg.hasArray())
                    out.write(msg.getArray(), msg.getOffset(), msg.getLength());
                else
                    msg.writePayload(out);
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


    protected class CompositeMessageIterator implements Iterator<Message> {
        protected int current_index;

        public boolean hasNext() {
            return current_index < index;
        }

        public Message next() {
            if(current_index >= msgs.length)
                throw new NoSuchElementException();
            return msgs[current_index++];
        }
    }


}
