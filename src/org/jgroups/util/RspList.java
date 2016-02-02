
package org.jgroups.util;


import org.jgroups.Address;

import java.util.*;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols. This class is unsynchronized
 */
public class RspList<T extends Object> implements Map<Address,Rsp<T>>, Iterable<Rsp<T>> {
    final Map<Address,Rsp<T>> rsps=new HashMap<>();


    public RspList() {

    }

    /** Adds a list of responses
     * @param responses Collection<Rsp>
     */
    public RspList(Collection<Rsp<T>> responses) {
        if(responses != null) {
            for(Rsp<T> rsp: responses) {
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
    public Rsp<T> get(Object key) {
        return rsps.get(key);
    }

    /**
     * Returns the value associated with address key
     * @param key
     * @return Object value
     */
    public T getValue(Object key) {
        Rsp<T> rsp=get(key);
        return rsp != null? rsp.getValue() : null;
    }

    public Rsp<T> put(Address key, Rsp<T> value) {
        return rsps.put(key, value);
    }

    public Rsp<T> remove(Object key) {
        return rsps.remove(key);
    }

    public void putAll(Map<? extends Address, ? extends Rsp<T>> m) {
        rsps.putAll(m);
    }

    public void clear() {
        rsps.clear();
    }

    public Set<Address> keySet() {
        return rsps.keySet();
    }

    public Collection<Rsp<T>> values() {
        return rsps.values();
    }

    public Set<Map.Entry<Address,Rsp<T>>> entrySet() {
        return rsps.entrySet();
    }



    public void addRsp(Address sender, T retval) {
        Rsp<T> rsp=get(sender);
        if(rsp != null) {
            rsp.setValue(retval);
            return;
        }
        rsps.put(sender, new Rsp<>(sender, retval));
    }


    public void addNotReceived(Address sender) {
        Rsp<T> rsp=get(sender);
        if(rsp == null)
            rsps.put(sender, new Rsp<T>(sender));
    }


    public boolean isReceived(Address sender) {
        Rsp<T> rsp=get(sender);
        return rsp != null && rsp.wasReceived();
    }

    public int numSuspectedMembers() {
        int num=0;
        Collection<Rsp<T>> values=values();
        for(Rsp<T> rsp: values) {
            if(rsp.wasSuspected())
                num++;
        }
        return num;
    }

    public int numReceived() {
        int num=0;
        Collection<Rsp<T>> values=values();
        for(Rsp<T> rsp: values) {
            if(rsp.wasReceived())
                num++;
        }
        return num;
    }

    /** Returns the first value in the response set. This is random, but we try to return a non-null value first */
    public T getFirst() {
        Collection<Rsp<T>> values=values();
        for(Rsp<T> rsp: values) {
            if(rsp.getValue() != null)
                return rsp.getValue();
        }
        return null;
    }


    /**
     * Returns the results from non-suspected members that are not null.
     */
    public List<T> getResults() {
        List<T> ret=new ArrayList<>(size());

        T val;
        for(Rsp<T> rsp: values()) {
            if(rsp.wasReceived() && (val=rsp.getValue()) != null)
                ret.add(val);
        }
        return ret;
    }


    public List<Address> getSuspectedMembers() {
        List<Address> retval=new ArrayList<>();
        for(Rsp<T> rsp: values()) {
            if(rsp.wasSuspected())
                retval.add(rsp.getSender());
        }
        return retval;
    }


    public boolean isSuspected(Address sender) {
        Rsp<T> rsp=get(sender);
        return rsp != null && rsp.wasSuspected();
    }


    public int size() {
        return rsps.size();
    }



    public String toString() {
        StringBuilder ret=new StringBuilder();
        for(Rsp<T> rsp: values()) {
            ret.append("[" + rsp + "]\n");
        }
        return ret.toString();
    }


    boolean contains(Address sender) {
        return containsKey(sender);
    }


    public Iterator<Rsp<T>> iterator() {
        return rsps.values().iterator();
    }
}
