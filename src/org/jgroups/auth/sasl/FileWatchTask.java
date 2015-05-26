package org.jgroups.auth.sasl;

import java.io.File;
import java.util.TimerTask;

/**
 * FileWatchTask. Polls a file for modifications and invokes a provided {@link FileObserver}
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class FileWatchTask extends TimerTask {
   private final File file;
   private long modified;
   private final FileObserver observer;

   public FileWatchTask(File file, FileObserver observer) {
      if (!file.exists()) {
         throw new IllegalArgumentException("File '" + file + "' does not exist");
      }
      this.file = file;
      this.modified = file.lastModified();
      this.observer = observer;
   }

   @Override
   public void run() {
      long modified = file.lastModified();
      if (this.modified != modified) {
         this.modified = modified;
         observer.fileChanged(file);
      }
   }

}
