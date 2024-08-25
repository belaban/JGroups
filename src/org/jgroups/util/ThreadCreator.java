package org.jgroups.util;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.*;

/**
 * @author Bela Ban
 * Helper class to create regular or virtual threads. Virtual threads are not supported for versions
 * less than Java 17.
 * @since 4.2.30
 */
public class ThreadCreator {
    private static final Log                  LOG=LogFactory.getLog(ThreadCreator.class);
    private static final MethodHandles.Lookup LOOKUP;
    private static final String               OF_VIRTUAL_NAME="java.lang.Thread$Builder$OfVirtual";
    private static final Class<?>             OF_VIRTUAL_CLASS;
    private static final MethodHandle         OF_VIRTUAL;
    private static final MethodHandle         CREATE_VTHREAD;
    private static final MethodHandle         EXECUTORS_NEW_VIRTUAL_THREAD_FACTORY;


    static {
        LOOKUP=MethodHandles.publicLookup();
        OF_VIRTUAL_CLASS=getOfVirtualClass();
        OF_VIRTUAL=getOfVirtualHandle();
        CREATE_VTHREAD=getCreateVThreadHandle();
        EXECUTORS_NEW_VIRTUAL_THREAD_FACTORY=getNewVirtualThreadFactoryHandle();
    }


    public static boolean hasVirtualThreads() {
        return CREATE_VTHREAD != null;
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


    protected static MethodHandle getNewVirtualThreadFactoryHandle() {
        MethodType type=MethodType.methodType(ExecutorService.class);
        String[] names={
          "newVirtualThreadPerTaskExecutor",  // jdk 18-21
          "newVirtualThreadExecutor",         // jdk 17
          "newUnboundedVirtualThreadExecutor" // jdk 15 & 16
        };

        for(int i=0; i < names.length; i++) {
            try {
                return LOOKUP.findStatic(Executors.class, names[i], type);
            }
            catch(Exception e) {
                String next=(i+1) < names.length? names[i+1] : "regular thread pool";
                LOG.debug("%s not found, falling back to %s", names[i], next);
            }
        }

        return null;
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


    public static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                   boolean virtual_threads, Log log) {
        return createThreadPool(min_threads, max_threads, keep_alive_time, "abort", new SynchronousQueue<>(),
                                new DefaultThreadFactory("threads", true, true),
                                virtual_threads, log);
    }

    public static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                   String rejection_policy,
                                                   BlockingQueue<Runnable> queue, final ThreadFactory factory,
                                                   boolean useVirtualThreads, Log log) {
        if(!useVirtualThreads || EXECUTORS_NEW_VIRTUAL_THREAD_FACTORY == null) {
            ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time,
                                                           TimeUnit.MILLISECONDS, queue, factory);
            RejectedExecutionHandler handler=Util.parseRejectionPolicy(rejection_policy);
            pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
            if(log != null)
                log.debug("thread pool min/max/keep-alive (ms): %d/%d/%d", min_threads, max_threads, keep_alive_time);
            return pool;
        }

        try {
            return (ExecutorService)EXECUTORS_NEW_VIRTUAL_THREAD_FACTORY.invokeExact();
        }
        catch(Throwable t) {
            throw new IllegalStateException(String.format("failed to create virtual thread pool: %s", t));
        }
    }

}
