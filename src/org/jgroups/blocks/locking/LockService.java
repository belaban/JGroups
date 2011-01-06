package org.jgroups.blocks.locking;

import java.util.concurrent.locks.Lock;

/**
 * @author Bela Ban
 */
public interface LockService {
    Lock getLock(String name);
}
