
package org.jgroups.util;


import org.jgroups.Address;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols. This class is unsynchronized
 */
public class RspList<T> extends HashMap<Address,Rsp<T>> implements Iterable<Rsp<T>> {
    private static final long serialVersionUID=6085009056724212815L;

    public RspList() {
    }

    public RspList(int size) {
        super(size);
    }

    public RspList(Map<Address,Rsp<T>> map) {
        putAll(map);
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


    public RspList<T> addRsp(Address sender, T retval) {
        Rsp<T> rsp=get(sender);
        if(rsp != null) {
            rsp.setValue(retval);
            return this;
        }
        put(sender, new Rsp<>(retval));
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
        return values().iterator();
    }
}
