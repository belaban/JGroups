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
    private static final String               OF_VIRTUAL_NAME="java.lang.Thread$Builder$OfVirtual";
    private static final MethodHandle         CREATE_VTHREAD;

    static {
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
            try {
                return (Thread)CREATE_VTHREAD.invokeExact(r);
            }
            catch(Throwable t) {
            }
        }
        return null;
    }

    protected static MethodHandle getCreateVThreadHandle() {
        try {
            Class<?> ofVirtualClass=getOfVirtualClass();
            if(ofVirtualClass == null)
                return null;
            MethodHandle ofVirtual=MethodHandles.publicLookup().findStatic(Thread.class, "ofVirtual",
                                                                              MethodType.methodType(ofVirtualClass));
            return MethodHandles.publicLookup().findVirtual(ofVirtualClass, "unstarted",
                                                            MethodType.methodType(Thread.class, Runnable.class))
              .bindTo(ofVirtual.invoke());
        }
        catch(Throwable ex) {
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

}
