package org.jgroups.util;

import org.jgroups.ReceiverAdapter;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Simple receiver which buffers all messages
 * @author Bela Ban
 * @version $Id: MyReceiver.java,v 1.3 2009/11/03 09:14:11 belaban Exp $
 */
public class MyReceiver extends ReceiverAdapter {
    protected final Collection<Message> msgs=new ConcurrentLinkedQueue<Message>();
    protected final String name;

    public MyReceiver(String name) {
        this.name=name;
    }

    public Collection<Message> getMsgs() {
        return msgs;
    }

    public void clear() {msgs.clear();}

    public void receive(Message msg) {
        System.out.println("[" + name + "] received message " + msg);
        msgs.add(msg);
    }

    public void viewAccepted(View new_view) {
        System.out.println("[" + name + "] view: " + new_view);
    }
}
