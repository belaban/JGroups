package org.jgroups.blocks;

import java.lang.reflect.Method;

/**
 * @author Bela Ban
 */
public interface MethodLookup {
    Method findMethod(short id);
}
