package org.jgroups.util;

import org.jgroups.logging.Log;

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
    protected volatile State      state=State.stopped;
    protected Thread              thread;
    protected boolean             daemon;
    protected long                join_timeout=100; // ms
    protected Log                 log;

    public enum State {stopped, stopping, running}

    public Runner(String name, Runnable function, Runnable stop_function) {
        this(new DefaultThreadFactory(name, true, true), name, function, stop_function);
    }

    public Runner(ThreadFactory factory, String thread_name, Runnable function, Runnable stop_function) {
        this.factory=factory;
        this.thread_name=thread_name;
        this.function=function;
        this.stop_function=stop_function;
    }

    public Thread  getThread()            {return thread;}
    public boolean isRunning()            {return state == State.running;}
    public boolean running()              {return state == State.running;}
    public State   state()                {return state;}
    public boolean daemon()               {return daemon;}
    public Runner  daemon(boolean d)      {daemon=d; return this;}
    public String  threadName()           {return thread_name;}
    public Runner  threadName(String n)   {thread_name=n; if(thread != null) thread.setName(n); return this;}
    public long    getJoinTimeout()       {return join_timeout;}
    public Runner  setJoinTimeout(long t) {join_timeout=t; return this;}
    public Runner  joinTimeout(long t)    {return setJoinTimeout(t);}
    public Log     log()                  {return log;}
    public Runner  log(Log l)             {log=l; return this;}


    public synchronized Runner start() {
        boolean rc=state(State.running);
        if(!rc)
            return this;
        String name=thread_name != null? thread_name : "runner";
        thread=factory != null? factory.newThread(this, name) : new Thread(this, name);
        if(thread.getClass() == Thread.class)
            thread.setDaemon(daemon);
        thread.start();
        return this;
    }

    public synchronized Runner stop() {
        boolean rc=state(State.stopping);
        if(!rc)
            return this;
        Thread tmp=thread;
        if(tmp != null) {
            tmp.interrupt();
            if(join_timeout > 0) {
                try {tmp.join(join_timeout);} catch(InterruptedException e) {}
            }
        }
        return this;
    }

    public void close() throws IOException {
        stop();
    }

    public void run() {
        for(;;) {
            while(state == State.running) {
                try {
                    function.run();
                }
                catch(Throwable t) {
                }
            }
            synchronized(this) {
                switch(state) {
                    case stopped:
                    case stopping:
                        if(state == State.stopping) {
                            runStopFuntion();
                            state(State.stopped);
                        }
                        return;
                }
            }
        }

    }

    @Override
    public String toString() {
        return String.format("%s[%s]", thread != null? thread.getName() : "", state);
    }

    // changes the state, needs to be synchronized
    protected boolean state(State new_state) {
        switch(this.state) {
            case stopped:
                switch(new_state) {
                    case stopped:  return false;
                    case stopping: return false; // not a valid transition
                    case running:
                        state=new_state; // called by start() - starts the worker thread
                        return true;
                }
            case stopping:
                switch(new_state) {
                    case stopped:
                        state=new_state; // called by run(), when the worker thread terminates
                        return false;
                    case stopping:
                        return false; // spurious stop(); is ignored
                    case running:
                        state=new_state; // stop() - start() sequence; the run() loop will continue
                        return false; // don't start a new worker thread!
                }
            case running:
                switch(new_state) {
                    case stopped:
                        return false; // invalid transition; we can't go directly from running -> stopped
                    case stopping:
                        state=new_state; // called by stop()
                        return true;
                    case running:
                        return false;
                }
        }
        throw new IllegalStateException(String.format("illegal transition from %s -> %s", state, new_state));
    }

    protected void runStopFuntion() {
        try {
            if(stop_function != null)
                stop_function.run();
        }
        catch(Throwable t) {
            if(log != null)
                log.error("%s: failed running stop_function: %s", thread_name, t.getMessage());
        }
    }

}
