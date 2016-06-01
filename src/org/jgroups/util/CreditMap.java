package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Maintains credits for senders, when credits fall below 0, a sender blocks until new credits have been received.
 * @author Bela Ban
 */
public class CreditMap {
    protected static final Log log = LogFactory.getLog(CreditMap.class);
    protected final long              max_credits;

    @GuardedBy("lock")
    protected final Map<Address,Long> credits=new HashMap<>();
    protected long                    min_credits;
    protected long                    accumulated_credits;
    protected final Lock              lock=new ReentrantLock();
    protected final Condition         credits_available=lock.newCondition();
    protected int                     num_blockings;
    protected final Average           avg_block_time=new Average(); // in ns


    public CreditMap(long max_credits) {
        this.max_credits=max_credits;
        min_credits=max_credits;
    }

    public long   getAccumulatedCredits() {return accumulated_credits;}
    public long   getMinCredits()         {return min_credits;}
    public int    getNumBlockings()       {return num_blockings;}
    public double getAverageBlockTime()   {return avg_block_time.getAverage() / 1000000.0;} // in ms

    public Set<Address> keys() {
        lock.lock();
        try {
            return credits.keySet();
        }
        finally {
            lock.unlock();
        }
    }

    public Long get(Address member) {
        lock.lock();
        try {
            return credits.get(member);
        }
        finally {
            lock.unlock();
        }
    }

    public Long remove(Address key) {
        lock.lock();
        try {
            Long retval=credits.remove(key);
            flushAccumulatedCredits();
            long new_min=computeLowestCredit();
            if(new_min > min_credits) {
                min_credits=new_min;
                credits_available.signalAll();
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public Long putIfAbsent(Address key) {
        lock.lock();
        try {
            flushAccumulatedCredits();
            Long val=credits.get(key);
            return val != null? val : credits.put(key, max_credits);
        }
        finally {
            lock.unlock();
        }
    }


    public List<Address> getMembersWithInsufficientCredits(long credit_needed) {
        List<Address> retval=new LinkedList<>();

        lock.lock();
        try {
            if(credit_needed > min_credits) {
                flushAccumulatedCredits();
                for(Map.Entry<Address,Long> entry: credits.entrySet()) {
                    if(entry.getValue() < credit_needed)
                        retval.add(entry.getKey());
                }
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }


    public List<Tuple<Address,Long>> getMembersWithCreditsLessThan(long min_credits) {
        List<Tuple<Address,Long>> retval=new LinkedList<>();

        lock.lock();
        try {
            flushAccumulatedCredits();
            for(Map.Entry<Address,Long> entry: credits.entrySet()) {
                if(entry.getValue() <= min_credits )
                    retval.add(new Tuple<>(entry.getKey(), entry.getValue()));
            }
            return retval;
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
            if(decrement(credits))
                return true;

            if(timeout <= 0)
                return false;

            log.trace("Waiting for credits: %d requested, %s - %d available", credits, this.credits, accumulated_credits);
            long start=System.nanoTime();
            try {
                credits_available.await(timeout, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
            }
            finally {
                num_blockings++;
                avg_block_time.add(System.nanoTime() - start);
            }
            
            return decrement(credits);
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

            boolean potential_update=val - accumulated_credits <= min_credits;
            decrementAndAdd(sender, new_credits);
            if(potential_update) {
                long new_min=computeLowestCredit();
                if(new_min > min_credits) {
                    min_credits=new_min;
                    credits_available.signalAll();
                }
            }
        }
        finally {
            lock.unlock();
        }
    }


    public void replenishAll() {
        lock.lock();
        try {
            flushAccumulatedCredits();
            for(Map.Entry<Address,Long> entry: credits.entrySet())
                entry.setValue(max_credits);
            min_credits=computeLowestCredit();
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    public void clear() {
        lock.lock();
        try {
            num_blockings=0;
            avg_block_time.clear();
            credits.clear();
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public void reset() {
        lock.lock();
        try {
            num_blockings=0;
            avg_block_time.clear();
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
            sb.append("min_credits=" + min_credits + ", accumulated=" + accumulated_credits);
        }
        finally {
            lock.unlock();
        }
        return sb.toString();
    }

    // need to be called with lock held
    protected boolean decrement(long credits) {
        if(credits <= min_credits) {
            accumulated_credits+=credits;
            min_credits-=credits;
            return true;
        }
        return false;
    }

    /** Needs to be called with lock held */
    protected long computeLowestCredit() {
        long lowest=max_credits;
        for(long cred: credits.values())
            lowest=Math.min(cred, lowest);
        return lowest;
    }

    public long computeLowestCreditWithAccumulated() {
        long lowest=max_credits;
        for(long cred: credits.values())
            lowest=Math.min(cred, lowest);
        return lowest - accumulated_credits;
    }

    /**
     * Decrements credits bytes from all elements and add new_credits to member (if non null).
     * The lowest credit needs to be greater than min_credits. Needs to be called with lock held
     * @param member The member to which new_credits are added. NOP if null
     * @param new_credits Number of bytes to add to member. NOP if 0.
     */
    protected void decrementAndAdd(Address member, long new_credits) {
        boolean replenish=member != null && new_credits > 0;

        if(accumulated_credits > 0) {
            for(Map.Entry<Address,Long> entry: this.credits.entrySet()) {
                entry.setValue(Math.max(0,entry.getValue() - accumulated_credits));
                if(replenish) {
                    Address tmp=entry.getKey();
                    if(tmp.equals(member))
                        entry.setValue(Math.min(max_credits,entry.getValue() + new_credits));
                }
            }
            accumulated_credits=0;
        }
        else {
            if(replenish) {
                Long val=this.credits.get(member);
                if(val != null)
                    this.credits.put(member, Math.min(max_credits,val + new_credits));
            }
        }
    }

    // Called with lock held
    protected void flushAccumulatedCredits() {
        if(accumulated_credits > 0) {
            for(Map.Entry<Address,Long> entry: this.credits.entrySet()) {
                entry.setValue(Math.max(0,entry.getValue() - accumulated_credits));
            }
            accumulated_credits=0;
        }
    }

    
}
