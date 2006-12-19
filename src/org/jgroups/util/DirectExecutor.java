package org.jgroups.util;

import java.util.concurrent.Executor;

/**
 * @author Bela Ban
 * @version $Id: DirectExecutor.java,v 1.2 2006/12/19 11:03:14 belaban Exp $
 */
public class DirectExecutor implements Executor {
    public void execute(Runnable command) {
        command.run();
    }
}
