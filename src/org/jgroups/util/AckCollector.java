package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.ViewId;

import java.util.*;

/**
 * Collects acks from a number of nodes, waits for all acks. Can also be time bounded
 * @author Bela Ban
 */
public class AckCollector {
    /** List<Object>: list of members from whom we haven't received an ACK yet */
    private final List<Object>     missing_acks;
    private final Promise<Boolean> all_acks_received=new Promise<Boolean>();
    private final Set<Address>     suspected_mbrs=new HashSet<Address>();
    private int                    expected_acks=0;


    public AckCollector() {
        missing_acks=new ArrayList<Object>();
        expected_acks=0;
    }

    public AckCollector(ViewId v, List<Object> l) {
        missing_acks=new ArrayList<Object>(l);
        if(v != null) {
            expected_acks=l != null? l.size() : 0;
        }
    }

    public synchronized String printMissing() {
        return missing_acks.toString();
    }


    public synchronized String printSuspected() {
        return suspected_mbrs.toString();
    }

    public synchronized void reset(Collection<Address> members) {
        suspected_mbrs.clear();
        missing_acks.clear();
        if(members != null && !members.isEmpty()) {
            missing_acks.addAll(members);
            expected_acks=members.size();
        }
        missing_acks.removeAll(suspected_mbrs);
        all_acks_received.reset();
    }

    public synchronized int size() {
        return missing_acks.size();
    }


    public synchronized int expectedAcks() {
        return expected_acks;
    }

    public synchronized void ack(Object member) {
        missing_acks.remove(member);
        if(missing_acks.isEmpty())
            all_acks_received.setResult(Boolean.TRUE);
    }

    public synchronized void suspect(Address member) {
        ack(member);
        suspected_mbrs.add(member);
    }

    public synchronized void unsuspect(Address member) {
        suspected_mbrs.remove(member);
    }

    public void handleView(View v) {
        if(v == null) return;
        List<Address> mbrs=v.getMembers();
        synchronized(this) {
            suspected_mbrs.retainAll(mbrs);
        }
    }

    public boolean waitForAllAcks() {
        if(missing_acks.isEmpty())
            return true;
        Object result=all_acks_received.getResult();
        return result != null && result instanceof Boolean && ((Boolean)result).booleanValue();
    }

    public boolean waitForAllAcks(long timeout) throws TimeoutException {
        if(missing_acks.isEmpty())
            return true;
        Object result=all_acks_received.getResultWithTimeout(timeout);
        return result != null && result instanceof Boolean && ((Boolean)result).booleanValue();
    }

    public String toString() {
        return "missing=" + printMissing();
    }
}
