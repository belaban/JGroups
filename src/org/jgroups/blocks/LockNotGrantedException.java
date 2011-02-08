package org.jgroups.blocks;

/**
 * This exception indicated that lock manager refused to give a lock on 
 * some resource.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LockNotGrantedException extends Exception {

    public LockNotGrantedException() {
        super();
    }

    public LockNotGrantedException(String s) {
        super(s);
    }
    
}