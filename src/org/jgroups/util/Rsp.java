// $Id: Rsp.java,v 1.2 2005/01/20 02:04:13 ovidiuf Exp $

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
        return "sender=" + sender + ", retval=" + retval + ", received=" +
            received + ", suspected=" + suspected;
    }
}

