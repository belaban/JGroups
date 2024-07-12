package org.jgroups.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

public class FileWatcher implements Runnable, AutoCloseable {

   static final Log LOG = LogFactory.getLog(FileWatcher.class);

   public static final int SLEEP = 2_000;
   private final Thread thread;
   private final ConcurrentHashMap<Path, Watched> watched;
   private boolean running = true;

   public FileWatcher() {
      watched = new ConcurrentHashMap<>();
      thread = new Thread(this, "FileWatcher");
      Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
      thread.start();
   }

   public void unwatch(Path path) {
      watched.remove(path);
      LOG.debug("Unwatched %s", path);
   }

   public void watch(Path path, Consumer<Path> callback) {
      watched.compute(path, (k, w) -> {
         if (w == null) {
            w = new Watched();
            try {
               w.lastModified = Files.getLastModifiedTime(path).toMillis();
            } catch (FileNotFoundException | NoSuchFileException e) {
               w.lastModified = -1;
               LOG.debug("File not found %s", path);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }
         w.watchers.add(callback);
         return w;
      });
      LOG.debug("Watching %s", path);
   }

   @Override
   public void run() {
      while (running) {
         try {
            Thread.sleep(SLEEP);
         } catch (InterruptedException e) {
            return;
         }
         if (!running) {
            return;
         }
         for (Map.Entry<Path, Watched> e : watched.entrySet()) {
            Watched w = e.getValue();
            try {
               long lastModified = Files.getLastModifiedTime(e.getKey()).toMillis();
               if (w.lastModified < lastModified) {
                  w.lastModified = lastModified;
                  for (Consumer<Path> c : w.watchers) {
                     c.accept(e.getKey());
                  }
               }
            } catch (FileNotFoundException | NoSuchFileException ex) {
               w.lastModified = -1;
            } catch (IOException ex) {
               throw new RuntimeException(ex);
            }
         }
      }
   }

   public void stop() {
      running = false;
      try {
         thread.join();
      } catch (InterruptedException e) {
         // Ignore
      }
   }

   @Override
   public void close() {
      stop();
   }

   static class Watched {
      long lastModified;
      List<Consumer<Path>> watchers = new ArrayList<>(2);
   }
}
