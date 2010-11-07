package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.ViewId;

import java.util.*;

/**
 * Collects acks from a number of nodes, waits for all acks. Can also be time bounded
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.19 2009/08/11 10:04:03 belaban Exp $
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

    public String printMissing() {
        synchronized(this) {
            return missing_acks.toString();
        }
    }

    @Deprecated
    public static String printReceived() {
        return "n/a";
    }

    public String printSuspected() {
        synchronized(this) {
            return suspected_mbrs.toString();
        }
    }

    public void reset(Collection<Address> members) {
        synchronized(this) {
            suspected_mbrs.clear();           
            missing_acks.clear();
            if(members != null && !members.isEmpty()) {
                missing_acks.addAll(members);
                expected_acks=members.size();
            }
            missing_acks.removeAll(suspected_mbrs);
            all_acks_received.reset();
        }
    }

    public int size() {
        synchronized(this) {
            return missing_acks.size();
        }
    }

    @Deprecated
    public static int receivedAcks() {
        return -1;
    }

    public int expectedAcks() {
        synchronized(this) {
            return expected_acks;
        }
    }

    public void ack(Object member) {
        synchronized(this) {
            missing_acks.remove(member);
            if(missing_acks.isEmpty())
                all_acks_received.setResult(Boolean.TRUE);
        }
    }

    public void suspect(Address member) {
        synchronized(this) {
            ack(member);
            suspected_mbrs.add(member);
        }
    }

    public void unsuspect(Address member) {
        synchronized(this) {
            suspected_mbrs.remove(member);
        }
    }

    public void handleView(View v) {
        if(v == null) return;
        Vector<Address> mbrs=v.getMembers();
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
