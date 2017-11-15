package org.jgroups;

import org.jgroups.util.ByteArray;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.util.function.Supplier;

/**
 * A {@link Message} without a payload; optimized for sending only headers (e.g. heartbeats in failure detection)
 * @author Bela Ban
 * @since  5.0
 */
public class EmptyMessage extends BaseMessage {

    public EmptyMessage(Address dest) {
        super(dest);
    }

    public EmptyMessage() {
    }

    public EmptyMessage(boolean create_headers) {
        super(create_headers);
    }

    public byte              getType() {return Message.EMPTY_MSG;}
    public Supplier<Message> create()  {return EmptyMessage::new;}

    public EmptyMessage copy(boolean copy_payload, boolean copy_headers) {
        EmptyMessage retval=new EmptyMessage();
        retval.dest=dest;
        retval.sender=sender;
        short tmp_flags=this.flags;
        byte tmp_tflags=this.transient_flags;
        retval.flags=tmp_flags;
        retval.transient_flags=tmp_tflags;
        retval.headers=copy_headers && headers != null? Headers.copy(this.headers) : createHeaders(Util.DEFAULT_HEADERS);
        return retval;
    }

    public boolean               hasPayload()                         {return false;}
    public boolean               hasArray()                           {return false;}
    public byte[]                getArray()                           {return null;}
    public int                   getOffset()                          {return 0;}
    public int                   getLength()                          {return 0;}
    public EmptyMessage          setArray(byte[] b, int off, int len) {throw new UnsupportedOperationException();}
    public EmptyMessage          setArray(ByteArray buf)              {throw new UnsupportedOperationException();}
    public <T extends Object> T  getObject()                          {throw new UnsupportedOperationException();}
    public EmptyMessage          setObject(Object obj)                {throw new UnsupportedOperationException();}
}
