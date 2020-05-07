package org.jgroups;

import org.jgroups.util.ByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * A {@link Message} without a payload; optimized for sending only headers (e.g. heartbeats in failure detection)
 * @author Bela Ban
 * @since  5.0
 */
public class EmptyMessage extends BaseMessage {

    public EmptyMessage() {
    }

    public EmptyMessage(Address dest) {
        super(dest);
    }

    public short                 getType()                            {return Message.EMPTY_MSG;}
    public Supplier<Message>     create()                             {return EmptyMessage::new;}
    public boolean               hasPayload()                         {return false;}
    public boolean               hasArray()                           {return false;}
    public byte[]                getArray()                           {return null;}
    public int                   getOffset()                          {return 0;}
    public int                   getLength()                          {return 0;}
    public EmptyMessage          setArray(byte[] b, int off, int len) {throw new UnsupportedOperationException();}
    public EmptyMessage          setArray(ByteArray buf)              {throw new UnsupportedOperationException();}
    public <T extends Object> T  getObject()                          {throw new UnsupportedOperationException();}
    public EmptyMessage          setObject(Object obj)                {throw new UnsupportedOperationException();}

    public void                  writePayload(DataOutput out) throws IOException {
        // no payload to write
    }

    public void                  readPayload(DataInput in) throws IOException, ClassNotFoundException {
        // no payload to read
    }
}
