
package org.jgroups.util;

import org.jgroups.Address;


/**
 * class that represents a response from a communication
 */
public class Rsp<T> {
    /** Flag that represents whether the response was received */
    protected boolean received;

    /** Flag that represents whether the response was suspected */
    protected boolean suspected;

    /** The sender of this response */
    protected Address sender;

    /** The value from the response */
    protected T retval;

    /** If there was an exception, this field will contain it */
    protected Throwable exception;


    public Rsp(Address sender) {
        this.sender=sender;
    }

    public Rsp(Address sender, boolean suspected) {
        this.sender=sender;
        this.suspected=suspected;
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

    public void setValue(T val) {
        this.retval=val;
        received=true;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable t) {
        this.exception=t;
        received=true;
    }

    public Address getSender() {
        return sender;
    }

    public boolean wasReceived() {
        return received;
    }

    public void setReceived(boolean received) {
        this.received=received;
        if(received)
            suspected=false;
    }

    public boolean wasSuspected() {
        return suspected;
    }

    public boolean setSuspected(boolean suspected) {
        boolean changed=!this.suspected && suspected;
        this.suspected=suspected;
        if(suspected)
            received=false;
        return changed;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("sender=").append(sender);
        if(retval != null)
            sb.append(", retval=").append(retval);
        if(exception != null)
            sb.append(", exception=").append(exception);
        sb.append(", received=").
          append(received).append(", suspected=").append(suspected);
        return sb.toString();
    }
}

