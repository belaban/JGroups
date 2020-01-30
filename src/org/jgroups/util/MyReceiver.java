package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.Receiver;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Generic receiver for a JChannel
 * @author Bela Ban
 * @since  3.3
 */
public class MyReceiver<T> implements Receiver, Closeable {
    protected final List<T> list=new CopyOnWriteArrayList<>();
    protected String        name;
    protected boolean       verbose;
    protected boolean       raw_msgs;

    public void receive(Message msg) {
        T obj=raw_msgs? (T)msg : (T)msg.getObject();
        list.add(obj);
        if(verbose) {
            System.out.println((name() != null? name() + ":" : "") + " received message from " + msg.getSrc() + ": " + obj);
        }
    }

    public MyReceiver<T> rawMsgs(boolean flag)      {this.raw_msgs=flag; return this;}
    public List<T>       list()                     {return list;}
    public MyReceiver<T> verbose(boolean flag)      {verbose=flag; return this;}
    public String        name()                     {return name;}
    public MyReceiver<T> name(String name)          {this.name=name; return this;}
    public MyReceiver<T> reset()                    {list.clear(); return this;}
    public int           size()                     {return list.size();}
    public void          close() throws IOException {reset();}
}
