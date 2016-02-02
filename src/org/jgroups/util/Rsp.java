
package org.jgroups.util;

import org.jgroups.Address;


/**
 * Class that represents a response from a communication
 */
public class Rsp<T> {
    /** Flag that represents whether the response was received */
    protected static final byte RECEIVED    = 1;

    /** Flag that represents whether the sender of the response was suspected */
    protected static final byte SUSPECTED   = 1 << 1;

    /** If true, the sender (below) could not be reached, e.g. if a site was down (only used by RELAY2) */
    protected static final byte UNREACHABLE = 1 << 2;

    protected byte          flags;

    /** The sender of this response */
    protected final Address sender;

    /** The value from the response */
    protected T             retval;

    /** If there was an exception, this field will contain it */
    protected Throwable     exception;


    public Rsp(Address sender) {
        this.sender=sender;
    }


    public Rsp(Address sender, T retval) {
        this.sender=sender;
        setValue(retval);
    }

    public Rsp(Address sender, Throwable t) {
        this.sender=sender;
        setException(t);
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof Rsp))
            return false;
        Rsp<T> other=(Rsp<T>)obj;
        if(sender != null)
            return sender.equals(other.sender);
        return other.sender == null;
    }

    public int hashCode() {
        return sender != null? sender.hashCode() : 0;
    }

    public T getValue() {
        return retval;
    }

    public Rsp<T> setValue(T val) {
        this.retval=val;
        setReceived();
        exception=null;
        return this;
    }

    public boolean hasException() {
        return exception != null;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable t) {
        if(t != null) {
            this.exception=t;
            setReceived();
            retval=null;
        }
    }

    public Address getSender() {
        return sender;
    }

    public boolean wasReceived() {
        return Util.isFlagSet(flags, RECEIVED);
    }

    public Rsp<T> setReceived() {
        this.flags=Util.setFlag(flags, RECEIVED);
        return this;
    }

    public boolean wasSuspected() {
        return Util.isFlagSet(flags, SUSPECTED);
    }

    public boolean setSuspected() {
        boolean changed=!wasSuspected();
        this.flags=Util.setFlag(flags, SUSPECTED);
        return changed;
    }

    public boolean wasUnreachable() {
        return Util.isFlagSet(flags, UNREACHABLE);
    }

    public boolean setUnreachable() {
        boolean changed=!wasUnreachable();
        this.flags=Util.setFlag(flags, UNREACHABLE);
        return changed;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("sender=").append(sender);
        if(retval != null)
            sb.append(", retval=").append(retval);
        if(exception != null)
            sb.append(", exception=").append(exception);
        sb.append(", received=").append(wasReceived()).append(", suspected=").append(wasSuspected());
        if(wasUnreachable())
            sb.append(" (unreachable)");
        return sb.toString();
    }
}

