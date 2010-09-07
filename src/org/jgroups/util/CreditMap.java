package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Maintains credits for senders, when credits fall below 0, a sender blocks until new credits have been received.
 * @author Bela Ban
 * @version $Id: CreditMap.java,v 1.1 2010/09/07 10:51:45 belaban Exp $
 */
public class CreditMap {
    protected final long              max_credits;
    @GuardedBy("lock")
    protected final Map<Address,Long> credits=new HashMap<Address,Long>();
    protected long                    min_credits;
    protected long                    accumulated_credits=0;
    protected final Lock              lock=new ReentrantLock();
    protected final Condition         credits_available=lock.newCondition();


    public CreditMap(long max_credits) {
        this.max_credits=max_credits;
        min_credits=max_credits;
    }

    public Set<Address> keys() {
        lock.lock();
        try {
            return credits.keySet();
        }
        finally {
            lock.unlock();
        }
    }

    public Long remove(Address key) {
        lock.lock();
        try {
            return credits.remove(key);
        }
        finally {
            lock.unlock();
        }
    }

    public Long putIfAbsent(Address key, Long value) {
        lock.lock();
        try {
            Long val=credits.get(key);
            return val != null? val : credits.put(key, value);
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Decrements credits bytes from all. Returns true if successful, or false if not. Blocks for timeout ms
     * (if greater than 0).
     *
     * @param credits Number of bytes to decrement from all members
     * @param timeout Number of milliseconds to wait until more credits have been received
     * @return True if decrementing credits bytes succeeded, false otherwise 
     */
    public boolean decrement(long credits, long timeout) {
        lock.lock();
        try {
            if(credits <= min_credits) {
                accumulated_credits+=credits;
                min_credits-=credits;
                return true;
            }




            return false;
        }
        finally {
            lock.unlock();
        }
    }
    

    public void replenish(Address sender, long new_credits) {
        if(sender == null)
            return;

        lock.lock();
        try {
            Long val=credits.get(sender);
            if(val == null)
                return;

            boolean update_min_credits=val.longValue() <= min_credits;
            decrementAndAdd(accumulated_credits, sender, new_credits);
            decrementAndAdd(accumulated_credits, sender, new_credits);
            if(update_min_credits) {
                min_credits=computeLowestCredit();
                credits_available.signalAll();
            }
            accumulated_credits=0;
        }
        finally {
            lock.unlock();
        }
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        lock.lock();
        try {
            for(Map.Entry<Address,Long> entry: credits.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            sb.append("min_credits=" + min_credits);
        }
        finally {
            lock.unlock();
        }
        return sb.toString();
    }

    /** Needs to be called with lock held */
    protected long computeLowestCredit() {
        long lowest=max_credits;
        for(long cred: credits.values())
            lowest=Math.min(cred, lowest);
        return lowest;
    }

    /**
     * Decrements credits bytes from all elements and add new_credits to member (if non null).
     * The lowest credit needs to be greater than min_credits. Needs to be called with lock held
     * @param credits Number of bytes to decrement from all. NOP is 0.
     * @param member The member to which new_credits are added. NOP if null
     * @param new_credits Number of bytes to add to member. NOP if 0.
     */
    protected void decrementAndAdd(long credits, Address member, long new_credits) {
        boolean replenish=member != null && new_credits > 0;

        if(credits > 0) {
            for(Map.Entry<Address,Long> entry: this.credits.entrySet()) {
                entry.setValue(Math.max(0, entry.getValue().longValue() - credits));
                if(replenish) {
                    Address tmp=entry.getKey();
                    if(tmp.equals(member))
                        entry.setValue(Math.min(max_credits, entry.getValue().longValue() + new_credits));
                }
            }
        }
        else {
            if(replenish) {
                Long val=this.credits.get(member);
                if(val != null)
                    this.credits.put(member, Math.min(max_credits, val.longValue() + new_credits));
            }
        }
    }

    
}
