package org.jgroups.util;

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

/**
 * @author Bela Ban
 * @version $Id: Metronome.java,v 1.1 2009/06/22 16:15:21 belaban Exp $
 */
public class Metronome implements Runnable {
    private final Set<Thread> threads=new HashSet<Thread>();
    private int tick=0;
    private volatile Thread worker=null;
    private int interval=10;
    private final Lock lock=new ReentrantLock();
    private final Condition cond=lock.newCondition();


    public Metronome(int interval) {
        this.interval=interval;
    }

    public void add(final Thread thread) {
        synchronized(threads) {
            threads.add(thread);
            if(worker == null || !worker.isAlive()) {
                worker=new Thread(this, "MetronomeThread");
                worker.setDaemon(true);
                worker.start();
            }
        }
    }

    public void remove(final Thread thread) {
        synchronized(threads) {
            threads.remove(thread);
        }
    }

    public void waitFor(int tick) {
        add(Thread.currentThread());
        while(true) {
            lock.lock();
            try {
                if(tick > this.tick) {
                    try {
                        cond.await();
                    }
                    catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                else
                    return;
            }
            finally {
                lock.unlock();
            }
        }
    }

    public void stop() {
        worker=null;
        synchronized(threads) {
            threads.clear();
        }
    }


    public void run() {
        while(worker != null && Thread.currentThread().equals(worker)) {
            boolean all_blocked=true;
            synchronized(threads) {
                if(threads.isEmpty())
                    break;

                for(Thread tmp: threads) {
                    Thread.State state=tmp.getState();
                    if(state != Thread.State.WAITING && state != Thread.State.BLOCKED) {
                        all_blocked=false;
                        System.out.println("state of " + tmp + ": " + state);
                        break;
                    }
                }
            }
            if(all_blocked) {
                lock.lock();
                try {
                    tick++;
                    cond.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }

            Util.sleep(interval);
        }
        worker=null;
    }
}
