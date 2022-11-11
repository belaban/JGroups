package org.jgroups.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import org.jgroups.blocks.Cache;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

/**
 * @since 14.0
 **/
public final class ThreadCreator {
   private static final Log log = LogFactory.getLog(ThreadCreator.class);
   private static final SPI INSTANCE = init();

   static private SPI init() {
      ServiceLoader<SPI> loader = ServiceLoader.load(SPI.class, ThreadCreator.class.getClassLoader());
      Iterator<SPI> i = loader.iterator();
      while (true) {
         try {
            SPI spi = i.next();
            log.debug("Using ThreadCreator %s", spi.getClass().getSimpleName());
            return spi;
         } catch (ServiceConfigurationError | UnsupportedClassVersionError e) {
            log.debug("Failed to initialize a ThreadCreator SPI", e);
         } catch (NoSuchElementException e) {
            throw new RuntimeException("Could not find a suitable SPI for " + SPI.class.getName());
         }
      }
   }


   public static boolean hasVirtualThreads() {
      return INSTANCE.hasVirtualThreads();
   }

   public static Thread createThread(Runnable target, String name, boolean createDaemons, boolean useVirtualThreads) {
      return INSTANCE.createThread(target, name, createDaemons, useVirtualThreads);
   }

   public static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                  String rejection_policy,
                                                  BlockingQueue<Runnable> queue, final ThreadFactory factory,
                                                  boolean useVirtualThreads, Log log) {
      return INSTANCE.createThreadPool(min_threads, max_threads, keep_alive_time, rejection_policy, queue, factory, useVirtualThreads, log);
   }


   public interface SPI {
      boolean hasVirtualThreads();

      Thread createThread(Runnable target, String name, boolean createDaemons, boolean useVirtualThreads);

      ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                       String rejection_policy,
                                       BlockingQueue<Runnable> queue, final ThreadFactory factory,
                                       boolean useVirtualThreads, Log log);
   }
}
