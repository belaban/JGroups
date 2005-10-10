package org.jgroups.util;

import org.jgroups.TimeoutException;
import org.jgroups.ViewId;
import org.jgroups.View;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.5 2005/10/10 12:23:10 belaban Exp $
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

    public java.util.List getMissing() {
        return missing_acks;
    }

    public Set getReceived() {
        return received_acks;
    }

    public ViewId getViewId() {
        return proposed_view;
    }

    public void reset(ViewId v, java.util.List l) {
        proposed_view=v;
        missing_acks.clear();
        received_acks.clear();
        if(l != null)
            missing_acks.addAll(l);
        missing_acks.removeAll(suspected_mbrs);
        all_acks_received.reset();
    }

    public int size() {
        return missing_acks.size();
    }

    public void ack(Object member) {
        missing_acks.remove(member);
        received_acks.add(member);
        if(missing_acks.size() == 0)
            all_acks_received.setResult(Boolean.TRUE);
    }

    public void suspect(Object member) {
        ack(member);
        suspected_mbrs.add(member);
    }

    public void unsuspect(Object member) {
        suspected_mbrs.remove(member);
    }

    public void handleView(View v) {
        if(v == null) return;
        Vector mbrs=v.getMembers();
        suspected_mbrs.retainAll(mbrs);
    }

    public boolean waitForAllAcks() {
        Object result=all_acks_received.getResult();
        if(result != null && result instanceof Boolean && ((Boolean)result).booleanValue())
            return true;
        return false;
    }

    public boolean waitForAllAcks(long timeout) throws TimeoutException {
        Object result=all_acks_received.getResultWithTimeout(timeout);
        if(result != null && result instanceof Boolean && ((Boolean)result).booleanValue())
            return true;
        return false;
    }

    public String toString() {
        return "missing=" + missing_acks + ", received=" + received_acks;
    }
}
