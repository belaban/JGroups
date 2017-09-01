package org.jgroups.util;

/**
 * Runs a given function in a loop (in a separate thread) until it is stopped
 * @author Bela Ban
 * @since  4.0
 */
public class Runner implements Runnable {
    protected final ThreadFactory factory;
    protected final String        thread_name;
    protected final Runnable      function;
    protected final Runnable      stop_function;
    protected volatile boolean    running;
    protected Thread              thread;


    public Runner(ThreadFactory factory, String thread_name, Runnable function, Runnable stop_function) {
        this.factory=factory;
        this.thread_name=thread_name;
        this.function=function;
        this.stop_function=stop_function;
    }

    public Thread  getThread()  {return thread;}
    public boolean isRunning()  {return running;}


    public synchronized void start() {
        if(running)
            return;
        if(thread == null || !thread.isAlive()) {
            String name=thread_name != null? thread_name : "runner";
            thread=factory != null? factory.newThread(this, name) : new Thread(this, name);
            running=true;
            thread.start();
        }
    }

    public synchronized void stop() {
        running=false;
        Thread tmp=thread;
        thread=null;
        if(tmp != null) {
            tmp.interrupt();
            if(tmp.isAlive()) {
                try {tmp.join(500);} catch(InterruptedException e) {}
            }
        }
        if(stop_function != null)
            stop_function.run();
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
