package org.jgroups.auth.sasl;

import java.io.File;

/**
 * FileObserver. A callback that is invoked when a file is changed. {@link FileWatchTask}
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public interface FileObserver {
   void fileChanged(File file);
}
