package org.jgroups.util;

import EDU.oswego.cs.dl.util.concurrent.Sync;

/**
 * @author Bela Ban
 * @version $Id: NullSync.java,v 1.1 2005/04/08 08:52:44 belaban Exp $
 */
public class NullSync implements Sync {

    public void acquire() throws InterruptedException {
    }

    public boolean attempt(long l) throws InterruptedException {
        return true;
    }

    public void release() {
    }
}
