package org.jgroups.util;

import org.jgroups.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Collects acks from a number of nodes, waits for all acks. Can also be time bounded
 * @author Bela Ban
 */
public class AckCollector {
    /** List of members from whom we haven't received an ACK yet */
    protected final List<Address>     missing_acks;
    protected final Promise<Boolean>  all_acks_received=new Promise<>();


    public AckCollector() {
        missing_acks=new ArrayList<>();
    }

    public AckCollector(Collection<Address> members) {
        missing_acks=new ArrayList<>(members != null? members.size() : 10);
        addAll(members);
    }

    public AckCollector(Address ... members) {
        missing_acks=new ArrayList<>(members != null? members.length : 10);
        addAll(members);
    }


    public synchronized void reset(Collection<Address> members) {
        missing_acks.clear();
        addAll(members);
        all_acks_received.reset();
    }

    public synchronized AckCollector reset(Collection<Address> expected_acks, Collection<Address> exclude) {
        missing_acks.clear();
        addAll(expected_acks, exclude);
        all_acks_received.reset();
        return this;
    }

    public synchronized AckCollector reset(Collection<Address> expected_acks, Address ... exclude) {
        missing_acks.clear();
        addAll(expected_acks, exclude);
        all_acks_received.reset();
        return this;
    }

    public synchronized void destroy() {
        missing_acks.clear();
        all_acks_received.setResult(null);
    }

    public synchronized int size() {
        return missing_acks.size();
    }

    public synchronized int expectedAcks() {
        return missing_acks.size();
    }

    public synchronized void ack(Address member) {
        if(member != null && missing_acks.remove(member) &&  missing_acks.isEmpty())
            all_acks_received.setResult(Boolean.TRUE);
    }

    public synchronized void ack(Address ... members) {
        for(Address member: members) {
            if(member != null && missing_acks.remove(member) && missing_acks.isEmpty())
                all_acks_received.setResult(Boolean.TRUE);
        }
    }

    public synchronized void ack(Collection<Address> members) {
        for(Address member: members) {
            if(member != null && missing_acks.remove(member) && missing_acks.isEmpty())
                all_acks_received.setResult(Boolean.TRUE);
        }
    }

    public void suspect(Address member) {
        ack(member); // checks for null member
    }

    public void suspect(Address ... members) {
        ack(members);
    }


    public void suspect(Collection<Address> members) {
        ack(members);
    }

    public boolean retainAll(Collection<Address> members) {
        if(members == null) return false;
        boolean retval=false;
        synchronized(this) {
            if((retval=missing_acks.retainAll(members)) && missing_acks.isEmpty())
                all_acks_received.setResult(Boolean.TRUE);
        }
        return retval;
    }

    public boolean waitForAllAcks() {
        if(missing_acks.isEmpty())
            return true;
        Boolean result=all_acks_received.getResult();
        return result != null && result;
    }

    public boolean waitForAllAcks(long timeout) throws TimeoutException {
        if(missing_acks.isEmpty())
            return true;
        Boolean result=all_acks_received.getResultWithTimeout(timeout);
        return result != null && result;
    }

    public String toString() {
        return printMissing();
    }

    public synchronized String printMissing() {
        return Util.printListWithDelimiter(missing_acks, ", ");
    }


    protected synchronized void addAll(Address ... members) {
        if(members == null)
            return;
        for(Address member: members)
            if(member != null && !missing_acks.contains(member))
                missing_acks.add(member);
    }

    protected synchronized void addAll(Collection<Address> members) {
        if(members == null)
            return;
        members.stream().filter(member -> member != null && !missing_acks.contains(member)).forEach(missing_acks::add);
    }

    protected synchronized void addAll(Collection<Address> members, Collection<Address> exclude) {
        if(members == null)
            return;
        members.stream().filter(m -> m != null && !missing_acks.contains(m) && (exclude != null && !exclude.contains(m)))
          .forEach(missing_acks::add);
    }

    protected synchronized void addAll(Collection<Address> members, Address ... exclude) {
        if(members == null)
            return;
        members.stream().filter(m -> m != null && !missing_acks.contains(m) && (exclude != null && !Util.contains(m, exclude)))
          .forEach(missing_acks::add);
    }
}
