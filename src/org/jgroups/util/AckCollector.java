package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.ViewId;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.16 2009/04/09 09:11:18 belaban Exp $
 */
public class AckCollector {
    /** List<Object>: list of members from whom we haven't received an ACK yet */
    private final List<Object>     missing_acks;
    private final Set<Object>      received_acks=new HashSet<Object>();
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

    public String printReceived() {
        synchronized(this) {
            return received_acks.toString();
        }
    }    

    public void reset(List<Address> members) {
        synchronized(this) {
            suspected_mbrs.clear();           
            missing_acks.clear();
            received_acks.clear();
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

    public int receivedAcks() {
        synchronized(this) {
            return received_acks.size();
        }
    }

    public int expectedAcks() {
        synchronized(this) {
            return expected_acks;
        }
    }

    public void ack(Object member) {
        synchronized(this) {
            missing_acks.remove(member);
            received_acks.add(member);
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
        return "missing=" + printMissing() + ", received=" + printReceived();
    }
}
