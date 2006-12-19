package org.jgroups.util;

import java.util.concurrent.Executor;

/**
 * @author Bela Ban
 * @version $Id: DirectExecutor.java,v 1.1 2006/12/19 11:02:52 belaban Exp $
 */
public class DirectExecutor implements Executor {
    public void execute(Runnable command) {
        command.run();
    }
}
