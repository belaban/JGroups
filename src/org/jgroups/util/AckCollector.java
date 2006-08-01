package org.jgroups.util;

import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.ViewId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.10 2006/08/01 16:08:01 belaban Exp $
 */
public class AckCollector {
    /** List<Object>: list of members from whom we haven't received an ACK yet */
    private final java.util.List missing_acks;
    private final Set            received_acks=new HashSet();
    private final Promise        all_acks_received=new Promise();
    private ViewId               proposed_view;
    private final Set            suspected_mbrs=new HashSet();


    public AckCollector() {
        missing_acks=new ArrayList();
    }

    public AckCollector(ViewId v, java.util.List l) {
        missing_acks=new ArrayList(l);
        proposed_view=v;
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

    public ViewId getViewId() {
        return proposed_view;
    }

    public void reset(ViewId v, java.util.List l) {
        synchronized(this) {
            proposed_view=v;
            missing_acks.clear();
            received_acks.clear();
            if(l != null)
                missing_acks.addAll(l);
            missing_acks.removeAll(suspected_mbrs);
            all_acks_received.reset();
        }
    }

    public int size() {
        synchronized(this) {
            return missing_acks.size();
        }
    }

    public void ack(Object member) {
        synchronized(this) {
            missing_acks.remove(member);
            received_acks.add(member);
            if(missing_acks.size() == 0)
                all_acks_received.setResult(Boolean.TRUE);
        }
    }

    public void suspect(Object member) {
        synchronized(this) {
            ack(member);
            suspected_mbrs.add(member);
        }
    }

    public void unsuspect(Object member) {
        synchronized(this) {
            suspected_mbrs.remove(member);
        }
    }

    public void handleView(View v) {
        if(v == null) return;
        Vector mbrs=v.getMembers();
        suspected_mbrs.retainAll(mbrs);
    }

    public boolean waitForAllAcks() {
        if(missing_acks.size() == 0)
            return true;
        Object result=all_acks_received.getResult();
        return result != null && result instanceof Boolean && ((Boolean)result).booleanValue();
    }

    public boolean waitForAllAcks(long timeout) throws TimeoutException {
        if(missing_acks.size() == 0)
            return true;
        Object result=all_acks_received.getResultWithTimeout(timeout);
        return result != null && result instanceof Boolean && ((Boolean)result).booleanValue();
    }

    public String toString() {
        return "missing=" + printMissing() + ", received=" + printReceived();
    }
}
