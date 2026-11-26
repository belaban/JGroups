package org.jgroups.blocks;

import java.lang.reflect.Method;

/**
 * Find a method given an ID
 * @author Bela Ban
 */
public interface MethodLookup {
    Method findMethod(short id);
}
