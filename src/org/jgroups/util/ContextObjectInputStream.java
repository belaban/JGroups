package org.jgroups.util;

import java.io.IOException;
import java.io.ObjectStreamClass;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;

/**
 * ObjectInputStream which sets a contact classloader for reading bytes into objects. Copied from
 * MarshalledValueInputStream of JBoss
 * @author Bela Ban
 * @version $Id: ContextObjectInputStream.java,v 1.1 2004/07/26 15:26:46 belaban Exp $
 */
public class ContextObjectInputStream extends ObjectInputStream {

    /**
     * A class wide cache of proxy classes populated by resolveProxyClass
     */
    private static HashMap classCache=new HashMap();


    /**
     * Creates a new instance of MarshalledValueOutputStream
     */
    public ContextObjectInputStream(InputStream is) throws IOException {
        super(is);
    }


    protected Class resolveClass(ObjectStreamClass v) throws IOException, ClassNotFoundException {
        String className=v.getName();
        Class resolvedClass=null;
        // Check the class cache first if it exists
        if(classCache != null) {
            synchronized(classCache) {
                resolvedClass=(Class)classCache.get(className);
            }
        }

        if(resolvedClass == null) {
            ClassLoader loader=Thread.currentThread().getContextClassLoader();
            try {
                resolvedClass=loader.loadClass(className);
            }
            catch(ClassNotFoundException e) {
                /* Use the super.resolveClass() call which will resolve array
                classes and primitives. We do not use this by default as this can
                result in caching of stale values across redeployments.
                */
                resolvedClass=super.resolveClass(v);
            }
            if(classCache != null) {
                synchronized(classCache) {
                    classCache.put(className, resolvedClass);
                }
            }
        }
        return resolvedClass;
    }
}

