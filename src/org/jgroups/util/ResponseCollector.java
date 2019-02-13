package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** Similar to AckCollector, but collects responses from cluster members, not just acks. Null is not a valid key.
 * @author Bela Ban
 */
public class ResponseCollector<T> {
    @GuardedBy("lock")
    private final Map<Address,T> responses;
    private final Lock           lock=new ReentrantLock(false);
    private final CondVar        cond=new CondVar(lock);


    /**
     *
     * @param members List of members from which we expect responses
     */
    public ResponseCollector(Collection<Address> members) {
        responses=members != null? new HashMap<>(members.size()) : new HashMap<>();
        reset(members);
    }

    public ResponseCollector(Address ... members) {
        responses=members != null? new HashMap<>(members.length) : new HashMap<>();
        reset(members);
    }

    public ResponseCollector() {
        responses=new HashMap<>();
    }

    public boolean add(Address member, T data) {
        if(member == null)
            return false;
        lock.lock();
        try {
            if(responses.containsKey(member)) {
                responses.put(member, data);
                cond.signal(true);
                return true;
            }
            return false;
        }
        finally {
            lock.unlock();
        }
    }

    public void remove(Address member) {
        if(member == null)
            return;
        lock.lock();
        try {
            if(responses.remove(member) != null)
                cond.signal(true);
        }
        finally {
            lock.unlock();
        }
    }

    public void remove(List<Address> members) {
        if(members == null || members.isEmpty())
            return;
        lock.lock();
        try {
            members.forEach(responses::remove);
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean retainAll(List<Address> members) {
        if(members == null || members.isEmpty())
            return false;
        lock.lock();
        try {
            if(responses.keySet().retainAll(members)) {
                cond.signal(true);
                return true;
            }
            return false;
        }
        finally {
            lock.unlock();
        }
    }

    public void suspect(Address member) {
        remove(member);
    }


    public boolean hasAllResponses() {
        lock.lock();
        try {
            return responses.isEmpty() || responses.entrySet().stream().allMatch(entry -> entry.getValue() != null);
        }
        finally {
            lock.unlock();
        }
    }

    public int numberOfValidResponses() {
        int retval=0;
        lock.lock();
        try {
            for(Map.Entry<Address,T> entry: responses.entrySet()) {
                if(entry.getValue() != null)
                    retval++;
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    /** Returns a list of members which didn't send a valid response */
    public List<Address> getMissing() {
        return responses.entrySet().stream().filter(entry -> entry.getValue() == null).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public List<Address> getValidResults() {
        return responses.entrySet().stream().filter(entry -> entry.getValue() != null).map(Map.Entry::getKey).collect(Collectors.toList());
    }


    public Map<Address,T> getResults() {
        return responses;
    }

    public int size() {
        lock.lock();
        try {
            return responses.size();
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Waits until all responses have been received, or until a timeout has elapsed.
     * @param timeout Number of milliseconds to wait max. This value needs to be greater than 0, or else
     * it will be adjusted to 2000
     * @return boolean True if all responses have been received within timeout ms, else false (e.g. if interrupted)
     */
    public boolean waitForAllResponses(long timeout) {
        if(timeout <= 0)
            timeout=2000L;
        return cond.waitFor(this::hasAllResponses, timeout, TimeUnit.MILLISECONDS);
    }

    public void reset() {
        reset((Collection<Address>)null);
    }

    public void reset(Collection<Address> members) {
        lock.lock();
        try {
            responses.clear();
            if(members != null) {
                for(Address mbr: members)
                    responses.put(mbr, null);
            }
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
    }

    public void reset(Address ... members) {
        lock.lock();
        try {
            responses.clear();
            if(members != null) {
                for(Address mbr: members)
                    responses.put(mbr, null);
            }
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
    }



    public String toString() {
        return String.format("%s, complete=%s", responses, hasAllResponses());
    }


}
