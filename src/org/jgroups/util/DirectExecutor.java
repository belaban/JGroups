package org.jgroups.util;

import java.util.concurrent.Executor;

/**
 * @author Bela Ban
 */
public class DirectExecutor implements Executor {
    public void execute(Runnable command) {
        command.run();
    }


}
