// $Id: RspList.java,v 1.7 2006/05/13 08:48:38 belaban Exp $

package org.jgroups.util;


import org.jgroups.Address;

import java.util.*;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols.
 */
public class RspList implements Map {

    /** Map<Address, Rsp> */
    final Map rsps=new HashMap();

    public RspList() {

    }

    /** Adds a lkist of responses
     * @param responses Collection<Rsp>
     */
    public RspList(Collection responses) {
        if(responses != null) {
            for(Iterator it=responses.iterator(); it.hasNext();) {
                Rsp rsp=(Rsp)it.next();
                rsps.put(rsp.getSender(), rsp);
            }
        }
    }


    public boolean isEmpty() {
        return rsps.isEmpty();
    }

    public boolean containsKey(Object key) {
        return rsps.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return rsps.containsValue(value);
    }

    /**
     * Returns the Rsp associated with address key
     * @param key Address (key)
     * @return Rsp
     */
    public Object get(Object key) {
        return rsps.get(key);
    }

    /**
     * Returns the value associated with address key
     * @param key
     * @return Object value
     */
    public Object getValue(Object key) {
        Rsp rsp=(Rsp)get(key);
        return rsp != null? rsp.getValue() : null;
    }

    public Object put(Object key, Object value) {
        return rsps.put(key, value);
    }

    public Object remove(Object key) {
        return rsps.remove(key);
    }

    public void putAll(Map m) {
        rsps.putAll(m);
    }

    public void clear() {
        rsps.clear();
    }

    public Set keySet() {
        return rsps.keySet();
    }

    public Collection values() {
        return rsps.values();
    }

    public Set entrySet() {
        return rsps.entrySet();
    }

    /**
     * Clears the response list
     * @deprecated Use {@link #clear()} instead
     */
    public void reset() {
        clear();
    }


    public void addRsp(Address sender, Object retval) {
        Rsp rsp=(Rsp)get(sender);
        if(rsp != null) {
            rsp.sender=sender;
            rsp.retval=retval;
            rsp.received=true;
            rsp.suspected=false;
            return;
        }
        rsps.put(sender, new Rsp(sender, retval));
    }


    public void addNotReceived(Address sender) {
        Rsp rsp=(Rsp)get(sender);
        if(rsp == null)
            rsps.put(sender, new Rsp(sender));
    }


    public void addSuspect(Address sender) {
        Rsp rsp=(Rsp)get(sender);
        if(rsp != null) {
            rsp.sender=sender;
            rsp.retval=null;
            rsp.received=false;
            rsp.suspected=true;
            return;
        }
        rsps.put(sender, new Rsp(sender, true));
    }


    public boolean isReceived(Address sender) {
        Rsp rsp=(Rsp)get(sender);
        if(rsp == null) return false;
        return rsp.received;
    }

    public int numSuspectedMembers() {
        int num=0;
        Rsp rsp;
        Collection values=values();
        for(Iterator it=values.iterator(); it.hasNext();) {
            rsp=(Rsp)it.next();
            if(rsp.wasSuspected())
                num++;
        }
        return num;
    }

    /** Returns the first value in the response set. This is random, but we try to return a non-null value first */
    public Object getFirst() {
        Collection values=values();
        for(Iterator it=values.iterator(); it.hasNext();) {
            Rsp rsp=(Rsp)it.next();
            if(rsp.getValue() != null)
                return rsp.getValue();
        }
        return null;
    }


    /**
     * Returns the results from non-suspected members that are not null.
     */
    public Vector getResults() {
        Vector ret=new Vector();
        Rsp rsp;
        Object val;

        for(Iterator it=values().iterator(); it.hasNext();) {
            rsp=(Rsp)it.next();
            if(rsp.wasReceived() && (val=rsp.getValue()) != null)
                ret.addElement(val);
        }
        return ret;
    }


    public Vector getSuspectedMembers() {
        Vector retval=new Vector();
        Rsp rsp;

        for(Iterator it=values().iterator(); it.hasNext();) {
            rsp=(Rsp)it.next();
            if(rsp.wasSuspected())
                retval.addElement(rsp.getSender());
        }
        return retval;
    }


    public boolean isSuspected(Address sender) {
        Rsp rsp=(Rsp)get(sender);
        if(rsp == null) return false;
        return rsp.suspected;
    }


    public int size() {
        return rsps.size();
    }

    /**
     * Returns the Rsp at index i
     * @param i The index
     * @return a Rsp
     * @throws ArrayIndexOutOfBoundsException
     * @deprecated Use {@link #entrySet()} or {@link #values()} instead
     */
    public Object elementAt(int i) throws ArrayIndexOutOfBoundsException {
        Set keys=new TreeSet(keySet());
        if(keys == null) return null;
        Object[] keys_array=keys.toArray();
        Object key=keys_array[i];
        return get(key);
    }


    public String toString() {
        StringBuffer ret=new StringBuffer();
        Rsp rsp;

        for(Iterator it=values().iterator(); it.hasNext();) {
            rsp=(Rsp)it.next();
            ret.append("[" + rsp + "]\n");
        }
        return ret.toString();
    }


    boolean contains(Address sender) {
        return containsKey(sender);
    }


}
