// $Id: Rsp.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


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
    Object  sender=null;
    /* the value from the response */
    Object  retval=null;


    Rsp(Object sender)
    {
        this.sender=sender;
    }

    Rsp(Object sender, boolean suspected)
    {
        this.sender=sender;
        this.suspected=suspected;
    }

    Rsp(Object sender, Object retval)
    {
        this.sender=sender;
        this.retval=retval;
        received=true;
    }

    public Object  getValue()
    {
        return retval;
    }

    public Object  getSender()
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

