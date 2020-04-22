package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.DefaultThreadFactory;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Simple and stupid async version of NoBundler. The main purpose is for a send() to return immediately, so
 * delivery of a message which itself sends a message is fast.
 * @author Bela Ban
 * @since  4.0.4
 */
@Experimental
public class AsyncNoBundler extends NoBundler {
    protected int max_threads=20;

    protected final ThreadPoolExecutor thread_pool;

    public AsyncNoBundler() {
        thread_pool=new ThreadPoolExecutor(0, max_threads,
                                           30000, TimeUnit.MICROSECONDS,
                                           new SynchronousQueue<>(),
                                           new DefaultThreadFactory("async-bundler", true, true),
                                           new ThreadPoolExecutor.CallerRunsPolicy());
        thread_pool.allowCoreThreadTimeOut(true);
    }

    @Override
    public void send(final Message msg) throws Exception {
        Runnable async_send=() -> {
            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(msg.size() + 10);
            try {
                sendSingleMessage(msg, out);
            }
            catch(Exception e) {
                log.error("failed sending message", e);
            }
        };
        thread_pool.execute(async_send);
    }
}
