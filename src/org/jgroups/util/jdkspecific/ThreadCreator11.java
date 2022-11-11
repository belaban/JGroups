package org.jgroups.util.jdkspecific;

import org.jgroups.logging.Log;
import org.jgroups.util.ShutdownRejectedExecutionHandler;
import org.jgroups.util.ThreadCreator;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.util.concurrent.*;

public class ThreadCreator11 implements ThreadCreator.SPI {

   public boolean hasVirtualThreads() {
      return false;
   }

   public Thread createThread(Runnable target, String name, boolean createDaemons, boolean ignored) {
      Thread t=new Thread(target, name);
      t.setDaemon(createDaemons);
      return t;
   }

   public ExecutorService createThreadPool(int min_threads,int max_threads,long keep_alive_time,
                                                  String rejection_policy,
                                                  BlockingQueue<Runnable> queue,final ThreadFactory factory,
                                                  boolean ignored, Log log) {
      ThreadPoolExecutor pool = new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS,
                                                       queue, factory);
      RejectedExecutionHandler handler = Util.parseRejectionPolicy(rejection_policy);
      pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
      log.debug("thread pool min/max/keep-alive (ms): %d/%d/%d", min_threads, max_threads, keep_alive_time);
      return pool;
   }
}
