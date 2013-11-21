package org.jgroups.util;

/**
 * Encapsulates the result of a state transfer. When buffer is set, then this is the result of a state transfer with
 * STATE_TRANSFER. When exception is non-null, then an exception occurred. When both fields are null, then the
 * (streaming) state transfer was successful.
 * @author Bela Ban
 * @since 3.0
 */
public class StateTransferResult {
    protected final byte[]    buffer;    // used with STATE_TRANSFER
    protected final Throwable exception; // when an exception occurred

    
    public StateTransferResult() {
        buffer=null;
        exception=null;
    }

    public StateTransferResult(byte[] buffer) {
        this.buffer=buffer;
        this.exception=null;
    }

    public StateTransferResult(Throwable t) {
        this.exception=t;
        this.buffer=null;
    }

    public boolean   hasBuffer()      {return buffer    != null;}
    public boolean   hasException()   {return exception != null;}
    public byte[]    getBuffer()      {return buffer;}
    public Throwable getException()   {return exception;}

    public String toString() {
        if(buffer != null)
            return Util.printBytes(buffer.length);
        else if(exception != null)
            return exception.toString();
        return "OK";
    }



}
