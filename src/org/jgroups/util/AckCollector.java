package org.jgroups.util;

import org.jgroups.TimeoutException;

import java.util.ArrayList;

/**
 * @author Bela Ban
 * @version $Id: AckCollector.java,v 1.2 2005/09/26 11:25:20 belaban Exp $
 */
public class AckCollector {
    /** List<Object>: list of members from whom we haven't received an ACK yet */
    final java.util.List missing_acks;
    final Promise all_acks_received=new Promise();

    public AckCollector(java.util.List l) {
        missing_acks=new ArrayList(l);
    }

    public void reset(java.util.List l) {
        missing_acks.clear();
        if(l != null)
            missing_acks.addAll(l);
        all_acks_received.reset();
    }

    public int size() {
        return missing_acks.size();
    }

    public void ack(Object member) {
        missing_acks.remove(member);
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
        return missing_acks.toString();
    }
}
