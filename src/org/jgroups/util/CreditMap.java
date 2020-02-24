package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;

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
    protected final long              max_credits;

    @GuardedBy("lock")
    protected final Map<Address,Long> credits=new HashMap<>();
    protected long                    min_credits;
    protected long                    accumulated_credits; // credits that need to be subtracted from each member
    protected final Lock              lock;
    protected final Condition         credits_available;
    protected int                     num_blockings;
    protected final Average           avg_block_time=new Average(); // in ns
    protected boolean                 done;


    public CreditMap(long max_credits) {
        this(max_credits, new ReentrantLock());
    }

    public CreditMap(long max_credits, final Lock lock) {
        this.max_credits=max_credits;
        this.min_credits=max_credits;
        this.lock=lock;
        this.credits_available=lock.newCondition();
    }

    public long   getAccumulatedCredits() {return accumulated_credits;}
    public long   getMinCredits()         {return min_credits;}
    public int    getNumBlockings()       {return num_blockings;}
    public double getAverageBlockTime()   {return avg_block_time.getAverage() / 1_000_000.0;} // in ms

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
                credits.entrySet().stream().filter(entry -> entry.getValue() < credit_needed)
                  .forEach(entry -> retval.add(entry.getKey()));
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
            credits.entrySet().stream().filter(entry -> entry.getValue() <= min_credits)
              .forEach(entry -> retval.add(new Tuple<>(entry.getKey(), entry.getValue())));

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
     *
     * @param msg The message to be sent
     * @param credits Number of bytes to decrement from all members
     * @param timeout Number of milliseconds to wait until more credits have been received
     * @return True if decrementing credits bytes succeeded, false otherwise
     */
    public boolean decrement(final Message msg, int credits, long timeout) {
        lock.lock();
        try {
            if(done)
                return false;
            if(decrement(credits))
                return true;

            if(timeout <= 0)
                return false;

            long start=System.nanoTime();
            try {
                credits_available.await(timeout, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
            }
            if(done)
                return false;
            num_blockings++;
            avg_block_time.add(System.nanoTime() - start);
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
            resetStats();
            credits.clear();
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /** Sets this credit to be done and releases all blocked threads. This is not revertable; a new credit
     * has to be created */
    public CreditMap reset() {
        lock.lock();
        try {
            if(!done) {
                done=true;
                credits_available.signalAll();
            }
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public void resetStats() {
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
                sb.append(entry.getKey()).append(": ").append(entry.getValue() - accumulated_credits).append("\n");
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
        if(min_credits - credits >= 0) {
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
     * Decrements credits bytes from all elements and adds new_credits to member (if non null).
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
