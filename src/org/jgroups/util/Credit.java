package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Maintains credits for a unicast destination. Used by flow control.
 * @author Bela Ban
 * @since  4.0.4
 */

public class Credit {
    protected final Lock      lock;
    protected final Condition credits_available;
    protected boolean         done; // set to true when not using this anymore; nobody will block
    protected long            credits_left;
    protected int             num_blockings;
    protected long            last_credit_request; // ns
    protected final Average   avg_blockings=new Average(); // ns


    public Credit(long credits) {
        this(credits, new ReentrantLock());
    }

    public Credit(long credits, final Lock lock) {
        this.credits_left=credits;
        this.lock=lock;
        this.credits_available=lock.newCondition();
    }

    public int getNumBlockings() {return num_blockings;}

    public long get() {
        lock.lock();
        try {
            return credits_left;
        }
        finally {
            lock.unlock();
        }
    }

    public double getAverageBlockTime() {return avg_blockings.getAverage();} // in ns
    public void   resetStats()          {num_blockings=0; avg_blockings.clear();}

    public boolean decrementIfEnoughCredits(final Message msg, int credits, long timeout) {
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
            avg_blockings.add(System.nanoTime() - start);
            return decrement(credits);
        }
        finally {
            lock.unlock();
        }
    }


    public long decrementAndGet(long credits, final long min_credits, final long max_credits) {
        lock.lock();
        try {
            credits_left=Math.max(0, credits_left - credits);
            if(min_credits - credits_left >= 0) {
                long credit_response=Math.min(max_credits, max_credits - credits_left);
                credits_left=max_credits;
                return credit_response;
            }
            return 0;
        }
        finally {
            lock.unlock();
        }
    }

    public void increment(long credits, final long max_credits) {
        lock.lock();
        try {
            credits_left=Math.min(max_credits, credits_left + credits);
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /** Sets this credit to be done and releases all blocked threads. This is not revertable; a new credit
     * has to be created */
    public Credit reset() {
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

    public boolean needToSendCreditRequest(final long max_block_time) {
        lock.lock();
        try {
            long current_time=System.nanoTime();
            // will most likely send a request the first time (last_credit_request is 0), unless nanoTime() is negative
            if(current_time - last_credit_request >= TimeUnit.NANOSECONDS.convert(max_block_time, TimeUnit.MILLISECONDS)) {
                last_credit_request=current_time;
                return true;
            }
            return false;
        }
        finally {
            lock.unlock();
        }
    }



    public String toString() {
        return String.valueOf(credits_left);
    }


    @GuardedBy("lock") protected boolean decrement(long credits) {
        if(credits_left - credits >= 0) {
            credits_left-=credits;
            return true;
        }
        return false;
    }
}
