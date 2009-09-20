// $Id: Rsp.java,v 1.8 2009/09/20 16:11:57 belaban Exp $

package org.jgroups.util;

import org.jgroups.Address;


/**
 * class that represents a response from a communication
 */
public class Rsp {
    /* flag that represents whether the response was received */
    boolean received=false;

    /* flag that represents whether the response was suspected */
    boolean suspected=false;

    /* The sender of this response */
    Address sender=null;

    /* the value from the response */
    Object retval=null;


    public Rsp(Address sender) {
        this.sender=sender;
    }

    public Rsp(Address sender, boolean suspected) {
        this.sender=sender;
        this.suspected=suspected;
    }

    public Rsp(Address sender, Object retval) {
        this.sender=sender;
        this.retval=retval;
        received=true;
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof Rsp))
            return false;
        Rsp other=(Rsp)obj;
        if(sender != null)
            return sender.equals(other.sender);
        return other.sender == null;
    }

    public int hashCode() {
        return sender != null? sender.hashCode() : 0;
    }

    public Object getValue() {
        return retval;
    }

    public void setValue(Object val) {
        this.retval=val;
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

    public void setSuspected(boolean suspected) {
        this.suspected=suspected;
        if(suspected)
            received=false;
    }

    public String toString() {
        return new StringBuilder("sender=").append(sender).append(", retval=").append(retval).append(", received=").
                append(received).append(", suspected=").append(suspected).toString();
    }
}

