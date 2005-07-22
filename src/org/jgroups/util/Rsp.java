// $Id: Rsp.java,v 1.3 2005/07/22 11:10:30 belaban Exp $

package org.jgroups.util;

import org.jgroups.Address;


/**
 * class that represents a response from a communication
 */
public class Rsp
{
    /* flag that represents whether the response was received */
    boolean received=false;
    /* flag that represents whether the response was suspected */
    boolean suspected=false;
    /* The sender of this response */
    Address  sender=null;
    /* the value from the response */
    Object  retval=null;


    Rsp(Address sender)
    {
        this.sender=sender;
    }

    Rsp(Address sender, boolean suspected)
    {
        this.sender=sender;
        this.suspected=suspected;
    }

    Rsp(Address sender, Object retval)
    {
        this.sender=sender;
        this.retval=retval;
        received=true;
    }

    public Object  getValue()
    {
        return retval;
    }

    public Address  getSender()
    {
        return sender;
    }

    public boolean wasReceived()
    {
        return received;
    }

    public boolean wasSuspected()
    {
        return suspected;
    }

    public String toString()
    {
        return new StringBuffer("sender=").append(sender).append(", retval=").append(retval).append(", received=").
                append(received).append(", suspected=").append(suspected).toString();
    }
}

