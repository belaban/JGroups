// $Id: MethodLookupJava.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.blocks;


import java.lang.reflect.Method;
import java.util.Vector;




public class MethodLookupJava implements MethodLookup {

    public Method findMethod(Class target_class, String method_name, Vector args) throws Exception {
        int len=args != null? args.size() : 0;
        Class[] formal_parms=new Class[len];
        Method retval;

        for(int i=0; i < len; i++)
            formal_parms[i]=args.elementAt(i).getClass();


        /* getDeclaredMethod() is a bit faster, but only searches for methods in the current
           class, not in superclasses */
        retval=target_class.getMethod(method_name, formal_parms);

        return retval;
    }
}
