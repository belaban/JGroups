package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Generic receiver for a JChannel
 * @param <T> T
 * @author Bela Ban
 * @since  3.3
 */
public class MyReceiver<T> implements Receiver, Closeable {
    protected final Lock               lock=new ReentrantLock();
    protected final List<T>            list=new FastArray<>(128);
    protected final List<View>         views=new FastArray<>(16);
    protected String                   name;
    protected boolean                  verbose;
    protected boolean                  raw_msgs;
    protected final Map<String,String> state=new HashMap<>();

    @Override public void receive(Message msg) {
        T obj=raw_msgs? (T)msg : (T)msg.getObject();
        lock.lock();
        try {
            list.add(obj);
        }
        finally {
            lock.unlock();
        }
        if(verbose)
            System.out.println((name() != null? name() + ":" : "") + " received message from " + msg.getSrc() + ": " + obj);
    }

    @Override public void viewAccepted(View new_view) {
        views.add(new_view);
        if(verbose)
            System.out.printf("-- %s: view is %s\n", name, new_view);
    }

    @Override
    public void getState(OutputStream out) throws Exception {
        DataOutputStream o=new DataOutputStream(out);
        synchronized(state) {
            o.writeInt(state.size());
            if(!state.isEmpty()) {
                for(Map.Entry<String,String> e: state.entrySet()) {
                    o.writeUTF(e.getKey());
                    o.writeUTF(e.getValue());
                }
            }
        }
    }

    @Override
    public void setState(InputStream input) throws Exception {
        DataInputStream in=new DataInputStream(input);
        Map<String,String> m=new HashMap<>();
        int size=in.readInt();
        for(int i=0; i < size; i++)
            m.put(in.readUTF(), in.readUTF());
        synchronized(state) {
            state.clear();
            state.putAll(m);
        }
    }

    public MyReceiver<T>      rawMsgs(boolean flag)      {this.raw_msgs=flag; return this;}
    public List<T>            list()                     {return list;}
    public List<String>       list(Function<T,String> f) {return list.stream().map(f).collect(Collectors.toList());}
    public List<View>         views()                    {return views;}
    public Map<String,String> state()                    {return state;}
    public MyReceiver<T>      verbose(boolean flag)      {verbose=flag; return this;}
    public String             name()                     {return name;}
    public MyReceiver<T>      name(String name)          {this.name=name; return this;}
    public MyReceiver<T>      reset()                    {list.clear(); views.clear(); return this;}
    public int                size()                     {return list.size();}
    public void               close() throws IOException {reset();}

    @Override
    public String toString() {
        return String.format("%d elements (%d views)", list.size(), views.size());
    }
}
