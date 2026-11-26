package org.jgroups.util;

import java.util.concurrent.Executor;

/**
 * Executor which runs the command on the caller's thread
 * @author Bela Ban
 */
public class DirectExecutor implements Executor {
    public void execute(Runnable command) {
        command.run();
    }
}
