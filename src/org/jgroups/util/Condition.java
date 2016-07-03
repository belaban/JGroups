package org.jgroups.util;

/**
 * @author Bela Ban
 * @since  3.6
 */
@FunctionalInterface
public interface Condition {
    /** Return true if the condition is met and false otherwise */
    boolean isMet();
}
