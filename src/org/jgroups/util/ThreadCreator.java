package org.jgroups.util;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Helper class to create regular or virtual threads. Virtual threads are not supported for versions less than Java 17.
 * @author Bela Ban
 */
public class ThreadCreator {
    private static final Log                  LOG=LogFactory.getLog(ThreadCreator.class);
    private static final MethodHandles.Lookup LOOKUP=MethodHandles.publicLookup();
    private static final String               OF_VIRTUAL_NAME="java.lang.Thread$Builder$OfVirtual";
    private static final Class<?>             OF_VIRTUAL_CLASS;
    private static final MethodHandle         OF_VIRTUAL;
    private static final MethodHandle         CREATE_VTHREAD;

    static {
        OF_VIRTUAL_CLASS=getOfVirtualClass();
        OF_VIRTUAL=getOfVirtualHandle();
        CREATE_VTHREAD=getCreateVThreadHandle();
    }

    public static boolean hasVirtualThreads() {
        return CREATE_VTHREAD != null;
    }

    public static Thread createThread(Runnable r, String name, boolean daemon, boolean virtual) {
        Thread t=null;
        if(virtual)
            t=newVirtualThread(r);
        if(t == null) {
            t=new Thread(r);
            t.setDaemon(daemon);
        }
        t.setName(name);
        return t;
    }

    protected static Thread newVirtualThread(Runnable r) {
        if(CREATE_VTHREAD != null) {
            // Thread.ofVirtual().unstarted()
            try {
                Object of=OF_VIRTUAL.invoke();
                return (Thread)CREATE_VTHREAD.invokeWithArguments(of, r);
            }
            catch(Throwable t) {
            }
        }
        return null;
    }

    protected static MethodHandle getCreateVThreadHandle() {
        MethodType type=MethodType.methodType(Thread.class, Runnable.class);
        try {
            return LOOKUP.findVirtual(OF_VIRTUAL_CLASS, "unstarted", type);
        }
        catch(Exception ex) {
            LOG.debug("%s.unstarted() not found, falling back to regular threads", OF_VIRTUAL_NAME);
        }
        return null;
    }

    protected static Class<?> getOfVirtualClass() {
        try {
            return Util.loadClass(OF_VIRTUAL_NAME, (Class<?>)null);
        }
        catch(ClassNotFoundException e) {
            LOG.debug("class %s not found", OF_VIRTUAL_NAME);
            return null;
        }
    }

    protected static MethodHandle getOfVirtualHandle() {
        try {
            return OF_VIRTUAL_CLASS != null?
              LOOKUP.findStatic(Thread.class, "ofVirtual", MethodType.methodType(OF_VIRTUAL_CLASS)) : null;
        }
        catch(Exception e) {
            return null;
        }
    }
}
