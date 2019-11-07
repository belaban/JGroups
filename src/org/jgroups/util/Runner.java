package org.jgroups.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * Runs a given function in a loop (in a separate thread) until it is stopped
 * @author Bela Ban
 * @since  4.0
 */
public class Runner implements Runnable, Closeable {
    protected final ThreadFactory factory;
    protected       String        thread_name;
    protected final Runnable      function;
    protected final Runnable      stop_function;
    protected volatile boolean    running;
    protected Thread              thread;
    protected boolean             daemon;
    protected long                join_timeout=100;


    public Runner(ThreadFactory factory, String thread_name, Runnable function, Runnable stop_function) {
        this.factory=factory;
        this.thread_name=thread_name;
        this.function=function;
        this.stop_function=stop_function;
    }

    public Thread  getThread()            {return thread;}
    public boolean isRunning()            {return running;}
    public boolean daemon()               {return daemon;}
    public Runner  daemon(boolean d)      {daemon=d; return this;}
    public String  threadName()           {return thread_name;}
    public Runner  threadName(String n)   {thread_name=n; if(thread != null) thread.setName(n); return this;}
    public long    getJoinTimeout()       {return join_timeout;}
    public Runner  setJoinTimeout(long t) {join_timeout=t; return this;}


    public synchronized Runner start() {
        if(running)
            return this;
        if(thread == null || !thread.isAlive()) {
            String name=thread_name != null? thread_name : "runner";
            thread=factory != null? factory.newThread(this, name) : new Thread(this, name);
            thread.setDaemon(daemon);
            running=true;
            thread.start();
        }
        return this;
    }

    public synchronized Runner stop() {
        running=false;
        Thread tmp=thread;
        thread=null;
        if(tmp != null) {
            tmp.interrupt();
            if(tmp.isAlive()) {
                try {tmp.join(join_timeout);} catch(InterruptedException e) {}
            }
        }
        if(stop_function != null)
            stop_function.run();
        return this;
    }

    public void close() throws IOException {
        stop();
    }


    public void run() {
        while(running) {
            try {
                function.run();
            }
            catch(Throwable t) {
            }
        }
    }


}
