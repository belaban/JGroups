package org.jgroups.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides a pool of output streams so we can do lock striping and have faster marshalling this way.
 * @author Bela Ban
 * @version $Id: MarshallerPool.java,v 1.1 2009/10/23 08:05:23 belaban Exp $
 */
public class MarshallerPool {
    final int pool_size;     // number of pools
    final int INITIAL_SIZE;  // initial size of each pool

    final ExposedByteArrayOutputStream[] outs;
    final ExposedDataOutputStream[] outputs;
    final Lock[] locks;

    public MarshallerPool(int pool_size, int initial_size) {
        this.pool_size=pool_size;
        INITIAL_SIZE=initial_size;

        outs=new ExposedByteArrayOutputStream[pool_size];
        for(int i=0; i < outs.length; i++)
            outs[i]=new ExposedByteArrayOutputStream(INITIAL_SIZE);

        outputs=new ExposedDataOutputStream[pool_size];
        for(int i=0; i < outputs.length; i++)
            outputs[i]=new ExposedDataOutputStream(outs[i]);

        locks=new Lock[pool_size];
        for(int i=0; i < locks.length; i++)
            locks[i]=new ReentrantLock();
    }


    /**
     * Returns a random output stream. To use it, the lock needs to be acquired.
     * When done, it also needs to be released again.
     * @return
     */
    public Triple<Lock,ExposedByteArrayOutputStream,ExposedDataOutputStream> getOutputStream() {
        int index=(int)Util.random(pool_size) -1;
        return new Triple<Lock,ExposedByteArrayOutputStream,ExposedDataOutputStream>(locks[index], outs[index], outputs[index]);
    }


    public int[] getCapacities() {
        int[] retval=new int[pool_size];
        for(int i=0; i < outs.length; i++)
            retval[i]=outs[i].getCapacity();
        return retval;
    }

    /** Closes all output streams. This releases the memory held by them */
    public void close() {
        for(int i=0; i < pool_size; i++) {
            try {
                locks[i].tryLock(2000, TimeUnit.MILLISECONDS);
                Util.close(outputs[i]);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                if(((ReentrantLock)locks[i]).isHeldByCurrentThread())
                    locks[i].unlock();
            }
        }
    }


}
