package org.jgroups.util;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * @author Bela Ban
 * @since  Helper class to create regular or virtual threads. Virtual threads are not supported for versions
 * less than Java 17.
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
        if(!virtual || CREATE_VTHREAD == null) {
            Thread t=new Thread(r, name);
            t.setDaemon(daemon);
            return t;
        }

        // Thread.ofVirtual().unstarted()
        try {
            Object of=OF_VIRTUAL.invoke();
            Thread t=(Thread)CREATE_VTHREAD.invokeWithArguments(of, r);
            t.setName(name);
            return t;
        }
        catch(Throwable t) {
        }

        // Thread.newThread(String name, int characteristics, Runnable task)  in JDKs 15 & 16
        try {
            return (Thread)CREATE_VTHREAD.invokeExact(name, 1, r);
        }
        catch(Throwable ex) {
        }
        return new Thread(r, name);
    }


    protected static MethodHandle getCreateVThreadHandle() {
        MethodType type=MethodType.methodType(Thread.class, Runnable.class);
        try {
            return LOOKUP.findVirtual(OF_VIRTUAL_CLASS, "unstarted", type);
        }
        catch(Exception ex) {
            LOG.debug("%s.unstarted() not found, trying Thread.newThread() (jdk 15/16)", OF_VIRTUAL_NAME);
        }

        // try Thread.newThread(String name, int characteristics, Runnable task)  in JDKs 15 & 16
        type=MethodType.methodType(Thread.class, String.class, int.class, Runnable.class);
        try {
            return LOOKUP.findStatic(Thread.class, "newThread", type);
        }
        catch(Exception ex) {
            LOG.debug("Thread.newThread() not found, falling back to regular threads");
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
