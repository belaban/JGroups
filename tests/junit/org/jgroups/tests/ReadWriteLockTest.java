package org.jgroups.tests;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;
import junit.framework.TestCase;

/**
 * @author bela
 * @version $Id: ReadWriteLockTest.java,v 1.1 2006/05/12 09:50:25 belaban Exp $
 */
public class ReadWriteLockTest extends TestCase {
    Sync rl, wl;

    protected void setUp() throws Exception {
        super.setUp();
        ReadWriteLock l=new ReentrantWriterPreferenceReadWriteLock();
        rl=l.readLock();
        wl=l.writeLock();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }


    public void testRelease() throws InterruptedException {
        rl.acquire();
        rl.release();
    }

    public void testUpgrade() throws InterruptedException {
        rl.acquire();
        rl.acquire();
        wl.acquire();
        wl.acquire();
        rl.acquire();
        rl.release();
        wl.acquire();
    }

}
