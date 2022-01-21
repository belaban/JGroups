
package org.jgroups;


import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A message that contains a batch of messages for use with BATCH protocol.  This message will wrap several
 * sequential messages so lower protocol layers only have to process them once.  This increases throughput for cases
 * such as when average message size is small, or there is a heavy processing cost (e.g. SEQUENCER).
 * <br/>
 * Similar to CompositeMessage but with some optimisations made for the specific use case.
 * <br/>
 * This class is unsynchronized; the envisaged use case is that an BatchMessage is created with a number of messages,
 * or messages are added, but then the instance is not modified anymore and sent.
 * @author Bela Ban, Chris Johnson
 * @since  5.x
 */
public class BatchMessage extends BaseMessage implements Iterable<Message> {
    protected Message[] msgs;
    protected int       index;    // index of the next message to be added
    protected Address   orig_src;


    protected static final MessageFactory mf=new DefaultMessageFactory();

    public BatchMessage() {
    }


    public BatchMessage(Address dest, int capacity) {
        super(dest);
        msgs=new Message[capacity];
    }

    public BatchMessage(Address dest, Address src, Message[] msgs, int index) {
        super(dest);
        this.orig_src = src;
        this.msgs = msgs;
        this.index = index;
    }


    public Supplier<Message>      create()                 {return BatchMessage::new;}
    public short                  getType()                {return Message.EARLYBATCH_MSG;}
    public boolean                hasPayload()             {return msgs != null && index > 0;}
    public boolean                hasArray()               {return false;}
    public int                    getNumberOfMessages()    {return index;}
    public int                    getOffset()              {throw new UnsupportedOperationException();}
    public byte[]                 getArray()               {throw new UnsupportedOperationException();}
    public BatchMessage setArray(byte[] b, int o, int l)   {throw new UnsupportedOperationException();}
    public BatchMessage setArray(ByteArray buf)            {throw new UnsupportedOperationException();}
    public BatchMessage setObject(Object obj)              {throw new UnsupportedOperationException();}
    public <T extends Object>  T  getObject()              {throw new UnsupportedOperationException();}
    public Message[]              getMessages()            {return msgs;}
    public Address                getOrigSender()          {return orig_src;}
    public BatchMessage           setOrigSender(Address s) {orig_src=s; return this;}



    public int getLength() {
        int total=0;
        for(int i=0; i < index && msgs != null; i++)
            total+=msgs[i].getLength();
        return total;
    }


    /** Adds the message at the end of the array. Increases the array if needed */
    public BatchMessage add(Message msg) {
        ensureSameDest(msg);
        ensureCapacity(index);
        msgs[index++]=Objects.requireNonNull(msg);
        return this;
    }

    public BatchMessage add(Message ... messages) {
        ensureCapacity(index + messages.length);
        for(Message msg: messages) {
            if (msg == null)
                break;
            msgs[index++] = Objects.requireNonNull(ensureSameDest(msg));
        }
        return this;
    }


    public <T extends Message> T get(int index) {
        return (T)msgs[index];
    }


    /** Create a shallow copy of this {@link BatchMessage}. */
    public BatchMessage copy(boolean copy_payload, boolean copy_headers) {
        BatchMessage retval=(BatchMessage)super.copy(copy_payload, copy_headers);
        if(copy_payload && msgs != null) {
            Message[] copy=new Message[msgs.length];
            for(int i=0; i < msgs.length; i++) {
                if(msgs[i] != null)
                    copy[i]=msgs[i];
            }
            retval.msgs=copy;
            retval.index=index;
            retval.orig_src=orig_src;
        }
        return retval;
    }


    public String toString() {
        return String.format("%s, %d message(s)", super.toString(), getNumberOfMessages());
    }

    public int size() {
        int retval=super.size() + Global.INT_SIZE + orig_src.serializedSize(); // length + src
        if(msgs != null) {
            for(int i=0; i < index; i++)
                retval+=msgs[i].size() + Global.SHORT_SIZE; // type
        }
        return retval;
    }

    public Iterator<Message> iterator() {
        return new BatchMessageIterator();
    }


    public void writePayload(DataOutput out) throws IOException {
        out.writeInt(index);
        Util.writeAddress(orig_src, out);
        if(msgs != null) {
            for(int i=0; i < index; i++) {
                Message msg=msgs[i];
                out.writeShort(msg.getType());
                msg.writeToNoAddrs(this.src(), out);
            }
        }
    }

    public void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        index=in.readInt();
        orig_src=Util.readAddress(in);
        if(index > 0) {
            msgs=new Message[index]; // a bit of additional space should we add byte arrays
            for(int i=0; i < index; i++) {
                short type=in.readShort();
                msgs[i]=mf.create(type).setDest(dest()).setSrc(orig_src);
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
            throw new IllegalStateException(String.format("message dest (%s) does not match dest of BatchMessage (%s)",
                                                          msg.dest(), dest));
        return msg;
    }


    protected class BatchMessageIterator implements Iterator<Message> {
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
