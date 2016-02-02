
package org.jgroups.util;


import org.jgroups.Address;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols. This class is unsynchronized
 */
public class RspList<T extends Object> implements Map<Address,Rsp<T>>, Iterable<Rsp<T>> {
    final Map<Address,Rsp<T>> rsps;


    public RspList() {
        rsps=new HashMap<>();
    }

    public RspList(int size) {
        rsps=new HashMap<>(size);
    }

    public RspList(Map<Address,Rsp<T>> map) {
        rsps=new HashMap<>(map != null? map.size() : 16);
        if(map != null)
            rsps.putAll(map);
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



    public RspList<T> addRsp(Address sender, T retval) {
        Rsp<T> rsp=get(sender);
        if(rsp != null) {
            rsp.setValue(retval);
            return this;
        }
        rsps.put(sender, new Rsp<>(retval));
        return this;
    }


    public boolean isReceived(Address sender) {
        Rsp<T> rsp=get(sender);
        return rsp != null && rsp.wasReceived();
    }

    public int numSuspectedMembers() {
        return (int)values().stream().filter(Rsp::wasSuspected).count();
    }

    public int numReceived() {
        return (int)values().stream().filter(Rsp::wasReceived).count();
    }

    /** Returns the first value in the response set. This is random, but we try to return a non-null value first */
    public T getFirst() {
        Optional<Rsp<T>> retval=values().stream().filter(rsp -> rsp.getValue() != null).findFirst();
        return retval.isPresent()? retval.get().getValue() : null;
    }


    /**
     * Returns the results from non-suspected members that are not null.
     */
    public List<T> getResults() {
        return values().stream().filter(rsp -> rsp.wasReceived() && rsp.getValue() != null)
          .collect(() -> new ArrayList<>(size()), (list,rsp) -> list.add(rsp.getValue()), (l,r) -> {});
    }


    public List<Address> getSuspectedMembers() {
        return entrySet().stream().filter(entry -> entry.getValue() != null && entry.getValue().wasSuspected())
          .map(Entry::getKey).collect(Collectors.toList());
    }


    public boolean isSuspected(Address sender) {
        Rsp<T> rsp=get(sender);
        return rsp != null && rsp.wasSuspected();
    }


    public int size() {
        return rsps.size();
    }



    public String toString() {
        return entrySet().stream()
          .collect(StringBuilder::new,
                   (sb,entry) -> sb.append("[").append(entry.getKey()).append(": ").append(entry.getValue()).append("]\n"),
                   (l,r)->{}).toString();
    }


    boolean contains(Address sender) {
        return containsKey(sender);
    }


    public Iterator<Rsp<T>> iterator() {
        return rsps.values().iterator();
    }
}
