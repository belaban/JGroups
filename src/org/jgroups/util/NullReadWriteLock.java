package org.jgroups.util;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;


/**
 * @author Bela Ban
 * @version $Id: NullReadWriteLock.java,v 1.1 2005/04/08 08:52:44 belaban Exp $
 */
public class NullReadWriteLock implements ReadWriteLock {
    Sync sync=new NullSync();

    public Sync readLock() {
        return sync;
    }

    public Sync writeLock() {
        return sync;
    }
}
