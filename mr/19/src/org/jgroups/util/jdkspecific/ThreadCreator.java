package org.jgroups.util.jdkspecific;

import org.jgroups.logging.Log;
import org.jgroups.util.ShutdownRejectedExecutionHandler;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.util.concurrent.*;

public class ThreadCreator {

   public static boolean hasVirtualThreads() {
      return true;
   }

   public static Thread createThread(Runnable target, String name, boolean createDaemons, boolean useVirtualThreads) {
      Thread t;
      if (useVirtualThreads) {
         t = Thread.ofVirtual().unstarted(target);
         t.setName(name);
      } else {
         t = new Thread(target, name);
         t.setDaemon(createDaemons);
      }
      return t;
   }

   public static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                  String rejection_policy,
                                                  BlockingQueue<Runnable> queue, final ThreadFactory factory,
                                                  boolean useVirtualThreads, Log log) {
      if (useVirtualThreads) {
         return Executors.newVirtualThreadPerTaskExecutor();
      } else {
         ThreadPoolExecutor pool = new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS,
               queue, factory);
         RejectedExecutionHandler handler = Util.parseRejectionPolicy(rejection_policy);
         pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
         log.debug("thread pool min/max/keep-alive (ms): %d/%d/%d", min_threads, max_threads, keep_alive_time);
         return pool;
      }
   }
}

