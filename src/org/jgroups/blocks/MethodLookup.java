// $Id: MethodLookup.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.blocks;

import java.lang.reflect.Method;
import java.util.Vector;

public interface MethodLookup {
    Method findMethod(Class target_class, String method_name, Vector args) throws Exception;
}
