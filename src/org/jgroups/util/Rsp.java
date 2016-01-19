
package org.jgroups.util;

import org.jgroups.Address;


/**
 * Class that represents a response from a communication
 */
public class Rsp<T> {
    /** Flag that represents whether the response was received */
    protected boolean       received;

    /** Flag that represents whether the sender of the response was suspected */
    protected boolean       suspected;

    /** If true, the sender (below) could not be reached, e.g. if a site was down (only used by RELAY2) */
    protected boolean       unreachable;

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
        if(this.getClass() != obj.getClass()) {
            return false;
        }
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

    public void setValue(T val) {
        this.retval=val;
        setReceived();
        exception=null;
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
        return received;
    }

    public void setReceived() {
        received=true;
    }

    public boolean wasSuspected() {
        return suspected;
    }

    public boolean setSuspected() {
        boolean changed=!suspected;
        suspected=true;
        return changed;
    }

    public boolean wasUnreachable() {
        return unreachable;
    }

    public boolean setUnreachable() {
        boolean changed=!unreachable;
        unreachable=true;
        return changed;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("sender=").append(sender);
        if(retval != null)
            sb.append(", retval=").append(retval);
        if(exception != null)
            sb.append(", exception=").append(exception);
        sb.append(", received=").append(received).append(", suspected=").append(suspected);
        if(unreachable)
            sb.append(" (unreachable)");
        return sb.toString();
    }
}

