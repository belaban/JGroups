package org.jgroups.util;

import java.util.concurrent.Executor;

/**
 * @author Bela Ban
 * @version $Id: DirectExecutor.java,v 1.5 2007/11/21 14:00:14 belaban Exp $
 */
public class DirectExecutor implements Executor {
    public void execute(Runnable command) {
        command.run();
    }


}
