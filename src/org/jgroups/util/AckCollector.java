package org.jgroups.util;

import org.jgroups.TimeoutException;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.3 2005/09/26 11:41:44 belaban Exp $
 */
public class AckCollector {
    /** List<Object>: list of members from whom we haven't received an ACK yet */
    final java.util.List missing_acks;
    final Set received_acks=new HashSet();
    final Promise all_acks_received=new Promise();

    public AckCollector() {
        missing_acks=new ArrayList();
    }

    public AckCollector(java.util.List l) {
        missing_acks=new ArrayList(l);
    }

    public java.util.List getMissing() {
        return missing_acks;
    }

    public Set getReceived() {
        return received_acks;
    }

    public void reset(java.util.List l) {
        missing_acks.clear();
        received_acks.clear();
        if(l != null)
            missing_acks.addAll(l);
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
