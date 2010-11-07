// $Id: RspList.java,v 1.10 2010/01/09 11:18:47 belaban Exp $

package org.jgroups.util;


import org.jgroups.Address;

import java.util.*;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols. This class is unsynchronized
 */
public class RspList implements Map<Address,Rsp> {
    public static final RspList EMPTY_RSP_LIST=new RspList();

    /** Map<Address, Rsp> */
    final Map<Address,Rsp> rsps=new HashMap<Address,Rsp>();


    public RspList() {

    }

    /** Adds a list of responses
     * @param responses Collection<Rsp>
     */
    public RspList(Collection<Rsp> responses) {
        if(responses != null) {
            for(Rsp rsp: responses) {
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
    public Rsp get(Object key) {
        return rsps.get(key);
    }

    /**
     * Returns the value associated with address key
     * @param key
     * @return Object value
     */
    public Object getValue(Object key) {
        Rsp rsp=get(key);
        return rsp != null? rsp.getValue() : null;
    }

    public Rsp put(Address key, Rsp value) {
        return rsps.put(key, value);
    }

    public Rsp remove(Object key) {
        return rsps.remove(key);
    }

    public void putAll(Map<? extends Address, ? extends Rsp> m) {
        rsps.putAll(m);
    }

    public void clear() {
        rsps.clear();
    }

    public Set<Address> keySet() {
        return rsps.keySet();
    }

    public Collection<Rsp> values() {
        return rsps.values();
    }

    public Set<Map.Entry<Address,Rsp>> entrySet() {
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
        Rsp rsp=get(sender);
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
        Rsp rsp=get(sender);
        if(rsp == null)
            rsps.put(sender, new Rsp(sender));
    }


    public void addSuspect(Address sender) {
        Rsp rsp=get(sender);
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
        Rsp rsp=get(sender);
        return rsp != null && rsp.received;
    }

    public int numSuspectedMembers() {
        int num=0;
        Collection<Rsp> values=values();
        for(Rsp rsp: values) {
            if(rsp.wasSuspected())
                num++;
        }
        return num;
    }

    public int numReceived() {
        int num=0;
        Collection<Rsp> values=values();
        for(Rsp rsp: values) {
            if(rsp.wasReceived())
                num++;
        }
        return num;
    }

    /** Returns the first value in the response set. This is random, but we try to return a non-null value first */
    public Object getFirst() {
        Collection<Rsp> values=values();
        for(Rsp rsp: values) {
            if(rsp.getValue() != null)
                return rsp.getValue();
        }
        return null;
    }


    /**
     * Returns the results from non-suspected members that are not null.
     */
    public Vector<Object> getResults() {
        Vector<Object> ret=new Vector<Object>();
        Object val;

        for(Rsp rsp: values()) {
            if(rsp.wasReceived() && (val=rsp.getValue()) != null)
                ret.addElement(val);
        }
        return ret;
    }


    public Vector<Address> getSuspectedMembers() {
        Vector<Address> retval=new Vector<Address>();
        for(Rsp rsp: values()) {
            if(rsp.wasSuspected())
                retval.addElement(rsp.getSender());
        }
        return retval;
    }


    public boolean isSuspected(Address sender) {
        Rsp rsp=get(sender);
        return rsp != null && rsp.suspected;
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
        Set<Address> keys=new TreeSet<Address>(keySet());
        Object[] keys_array=keys.toArray();
        Object key=keys_array[i];
        return get(key);
    }


    public String toString() {
        StringBuilder ret=new StringBuilder();
        for(Rsp rsp: values()) {
            ret.append("[" + rsp + "]\n");
        }
        return ret.toString();
    }


    boolean contains(Address sender) {
        return containsKey(sender);
    }


}
