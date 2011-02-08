package org.jgroups.blocks;

/**
 * This exception indicated that lock manager refused to release a lock on 
 * some resource.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class LockNotReleasedException extends Exception {

    public LockNotReleasedException() {
        super();
    }

    public LockNotReleasedException(String s) {
        super(s);
    }
    
}
