package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Pool;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class PoolTest {


    public void testSimpleGet() throws Exception {
        Pool<Integer> pool=new Pool<>(4, new Pool.Creator<Integer>() {
            public Integer create() {return (int)Util.random(10);}
        });
        List<Lock> locks=grab(pool, 3);
        System.out.println("pool = " + pool);
        assert pool.getNumLocked() == 3;
        assert locks.size() == 3;
    }


    public void testGet() throws Exception {
        Pool<Integer> pool=new Pool<>(4, new Pool.Creator<Integer>() {
            public Integer create() {return (int)Util.random(10);}
        });

        List<Lock> locks=grab(pool, 3);
        System.out.println("pool: " + pool);

        assert pool.getNumLocked() == 3;
        locks.addAll(grab(pool,3));

        assert locks.size() == 4;

        Pool.Element el=pool.get();
        assert el.getLock() == null;
    }

    public void testGetAndUnlock() {
        Pool<Integer> pool=new Pool<>(4, new Pool.Creator<Integer>() {
            public Integer create() {return (int)Util.random(10);}
        });
        List<Lock> locks=new ArrayList<>(4);
        for(int i=0; i < 4; i++)
            locks.add(pool.get().getLock());
        System.out.println("locks = " + locks);

        // the same thread can lock the same lock multiple times
        assert pool.getNumLocked() > 0 && pool.getNumLocked() <= 4;
        for(Lock lock: locks)
            lock.unlock();
        System.out.println("pool = " + pool);
        assert pool.getNumLocked() == 0;
    }


    protected static <T>List<Lock> grab(Pool<T> pool, int num) throws Exception {
        List<Lock> retval=new ArrayList<>(num);
        for(int i=0; i < num; i++) {
            Grabber grabber=new Grabber(pool);
            grabber.start();
            grabber.join();
            retval.addAll(grabber.getLocks());
        }
        return retval;
    }


    protected static class Grabber<T> extends Thread {
        protected final Pool<T>     pool;
        protected final List<Lock>  locks=new ArrayList<>();

        public Grabber(Pool<T> pool) {
            this.pool=pool;
        }

        public List<Lock> getLocks() {
            return locks;
        }

        public void run() {
            Pool.Element element=pool.get();
            if(element.getLock() != null)
                locks.add(element.getLock());
        }
    }
}
