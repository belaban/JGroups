// $Id: RspList.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


import java.util.Vector;
import org.jgroups.Address;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols.
 */
public class RspList {
    Vector rsps=new Vector();


    public void reset()
    {
        rsps.removeAllElements();
    }


    public void addRsp(Address sender, Object retval)
    {
        Rsp rsp=find(sender);

        if(rsp != null)
        {
            rsp.sender=sender; rsp.retval=retval; rsp.received=true; rsp.suspected=false;
            return;
        }
        rsps.addElement(new Rsp(sender, retval));
    }


    public void addNotReceived(Address sender)
    {
        Rsp rsp=find(sender);

        if(rsp == null)
            rsps.addElement(new Rsp(sender));
    }



    public void addSuspect(Address sender)
    {
        Rsp rsp=find(sender);

        if(rsp != null)
        {
            rsp.sender=sender; rsp.retval=null; rsp.received=false; rsp.suspected=true;
            return;
        }
        rsps.addElement(new Rsp(sender, true));
    }


    public boolean isReceived(Address sender)
    {
        Rsp rsp=find(sender);

        if(rsp ==null) return false;
        return rsp.received;
    }


    public int numSuspectedMembers()
    {
        int  num=0;
        Rsp  rsp;

        for(int i=0; i < rsps.size(); i++)
        {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.wasSuspected())
                num++;
        }
        return num;
    }


    public Object getFirst()
    {
        return rsps.size() > 0 ? ((Rsp)rsps.elementAt(0)).getValue() : null;
    }


    /** Returns the results from non-suspected members that are not null. */
    public Vector getResults()
    {
        Vector ret=new Vector();
        Rsp    rsp;
        Object val;

        for(int i=0; i < rsps.size(); i++)
        {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.wasReceived() && (val=rsp.getValue()) != null)
                ret.addElement(val);
        }
        return ret;
    }


    public Vector getSuspectedMembers()
    {
        Vector retval=new Vector();
        Rsp    rsp;

        for(int i=0; i < rsps.size(); i++)
        {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.wasSuspected())
                retval.addElement(rsp.getSender());
        }
        return retval;
    }


    public boolean isSuspected(Address sender)
    {
        Rsp rsp=find(sender);

        if(rsp ==null) return false;
        return rsp.suspected;
    }


    public Object get(Address sender)
    {
        Rsp rsp=find(sender);

        if(rsp == null) return null;
        return rsp.retval;
    }


    public int size()
    {
        return rsps.size();
    }

    public Object elementAt(int i) throws ArrayIndexOutOfBoundsException
    {
        return rsps.elementAt(i);
    }


    public String toString()
    {
        StringBuffer ret=new StringBuffer();
        Rsp          rsp;
        Address      sender;

        for(int i=0; i < rsps.size(); i++)
        {
            rsp=(Rsp)rsps.elementAt(i);
            ret.append("[" + rsp + "]\n");
        }
        return ret.toString();
    }





    boolean contains(Address sender)
    {
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++)
        {
            rsp=(Rsp)rsps.elementAt(i);

            if(rsp.sender != null && sender != null && rsp.sender.equals(sender))
                return true;
        }
        return false;
    }


    Rsp find(Address sender)
    {
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++)
        {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.sender != null && sender != null && rsp.sender.equals(sender))
                return rsp;
        }
        return null;
    }




}
