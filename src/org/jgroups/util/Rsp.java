
package org.jgroups.util;

import org.jgroups.Address;


/**
 * Class that represents a response from a communication
 */
public class Rsp<T> {
    /** Flag that represents whether the response was received */
    protected static final byte RECEIVED     = 1;

    /** Flag that represents whether the sender of the response was suspected */
    protected static final byte SUSPECTED    = 1 << 1;

    /** If true, the sender (below) could not be reached, e.g. if a site was down (only used by RELAY2) */
    protected static final byte UNREACHABLE  = 1 << 2;

    /** Set when the value is an exception */
    protected static final byte IS_EXCEPTION = 1 << 3;

    protected byte          flags;

    /** The sender of this response */
    protected final Address sender;

    /** The value from the response (or the exception) */
    protected Object        value; // untyped, to be able to hold both T and Throwable


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
        return (T)value;
    }

    public Rsp<T> setValue(T val) {
        this.value=val;
        setReceived();
        this.flags=Util.clearFlags(flags, IS_EXCEPTION); // clear the exception flag just in case it is set
        return this;
    }

    public boolean hasException() {
        return Util.isFlagSet(flags, IS_EXCEPTION);
    }

    public Throwable getException() {
        return hasException()? (Throwable)value : null;
    }

    public Rsp<T> setException(Throwable t) {
        if(t != null) {
            this.value=t;
            setReceived();
            this.flags=Util.setFlag(flags, IS_EXCEPTION);
        }
        return this;
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
        if(value != null) {
            if(!hasException())
                sb.append(", value=").append(value);
            else
                sb.append(", exception=").append(getException());
        }
        sb.append(", received=").append(wasReceived()).append(", suspected=").append(wasSuspected());
        if(wasUnreachable())
            sb.append(" (unreachable)");
        return sb.toString();
    }
}

