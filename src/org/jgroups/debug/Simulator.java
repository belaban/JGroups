package org.jgroups.debug;

import org.jgroups.Address;
import org.jgroups.ChannelException;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.TimeScheduler;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Tests one or more protocols independently. Look at org.jgroups.tests.FCTest for an example of how to use it.
 * @author Bela Ban
 * @version $Id: Simulator.java,v 1.13 2008/10/23 01:09:52 rachmatowicz Exp $
 */
public class Simulator {
    private Protocol[] protStack=null;
    private ProtocolAdapter ad=new ProtocolAdapter();
    ProtocolStack prot_stack=null;
    private Receiver r=null;
    private Protocol top=null, bottom=null;
    private Queue send_queue=new Queue();
    private Thread send_thread;
    private Queue recv_queue=new Queue();
    private Thread recv_thread;


    /** HashMap from Address to Simulator. */
    private final HashMap addrTable=new HashMap();
    private Address local_addr=null;
    private View view;

    public interface Receiver {
        void receive(Event evt);
    }


    public void setProtocolStack(Protocol[] stack) {
        this.protStack=stack;
        this.protStack[0].setUpProtocol(ad);
        this.protStack[this.protStack.length-1].setDownProtocol(ad);
        top=protStack[0];
        bottom=this.protStack[this.protStack.length-1];

        try {
            prot_stack=new ProtocolStack();
        } catch (ChannelException e) {           
            e.printStackTrace();
        }

        if(protStack.length > 1) {
            for(int i=0; i < protStack.length; i++) {
                Protocol p1=protStack[i];
                p1.setProtocolStack(prot_stack);
                Protocol p2=i+1 >= protStack.length? null : protStack[i+1];
                if(p2 != null) {
                    p1.setDownProtocol(p2);
                    p2.setUpProtocol(p1);
                }
            }
        }
    }

    public String dumpStats() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < protStack.length; i++) {
            Protocol p1=protStack[i];
            sb.append(p1.getName()).append(":\n").append(p1.dumpStats()).append("\n");
        }
        return sb.toString();
    }

    public void addMember(Address addr) {
        addMember(addr, this);
    }

    public void addMember(Address addr, Simulator s) {
        addrTable.put(addr, s);
    }

    public void setLocalAddress(Address addr) {
        this.local_addr=addr;
    }

    public void setView(View v) {
        this.view=v;
    }

    public void setReceiver(Receiver r) {
        this.r=r;
    }

    public Object send(Event evt) {
        return top.down(evt);
    }

    public void receive(Event evt) {
        try {
            Event copy;
            if(evt.getType() == Event.MSG && evt.getArg() != null) {
                copy=new Event(Event.MSG, ((Message)evt.getArg()).copy());
            }
            else
                copy=evt;

            recv_queue.add(copy);
        }
        catch(QueueClosedException e) {
        }
    }

    public void start() throws Exception {
       if(local_addr == null)
            throw new Exception("local_addr has to be non-null");
        if(protStack == null)
            throw new Exception("protocol stack is null");

        for(int i=0; i < protStack.length; i++) {
            Protocol p=protStack[i];
            p.setProtocolStack(prot_stack);
        }

        for(int i=0; i < protStack.length; i++) {
            Protocol p=protStack[i];
            p.init();
        }

        for(int i=0; i < protStack.length; i++) {
            Protocol p=protStack[i];
            p.start();
        }

		// moved event processing to follow stack init (JGRP-843)
        bottom.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        if(view != null) {
            Event view_evt=new Event(Event.VIEW_CHANGE, view);
            bottom.up(view_evt);
            top.down(view_evt);
        }


        send_thread=new Thread() {
            public void run() {
                Event evt;
                while(send_thread != null) {
                    try {
                        evt=(Event)send_queue.remove();
                        if(evt.getType() == Event.MSG) {
                            Message msg=(Message)evt.getArg();
                            Address dst=msg.getDest();
                            if(msg.getSrc() == null)
                                ((Message)evt.getArg()).setSrc(local_addr);
                            Simulator s;
                            if(dst == null) {
                                for(Iterator it=addrTable.values().iterator(); it.hasNext();) {
                                    s=(Simulator)it.next();
                                    s.receive(evt);
                                }
                            }
                            else {
                                s=(Simulator)addrTable.get(dst);
                                if(s != null)
                                    s.receive(evt);
                            }
                        }
                    }
                    catch(QueueClosedException e) {
                        send_thread=null;
                        break;
                    }
                }
            }
        };
        send_thread.start();


        recv_thread=new Thread() {
            public void run() {
                Event evt;
                while(recv_thread != null) {
                    try {
                        evt=(Event)recv_queue.remove();
                        bottom.up(evt);
                    }
                    catch(QueueClosedException e) {
                        recv_thread=null;
                        break;
                    }
                }
            }
        };
        recv_thread.start();
    }

    public void stop() {
        recv_thread=null;
        recv_queue.close(false);
        send_thread=null;
        send_queue.close(false);
        if(ad != null) {
            try {
                ad.getTimer().stop();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }




    class ProtocolAdapter extends TP {

        ProtocolAdapter() {
            timer=new TimeScheduler();
        }

        public TimeScheduler getTimer() {
            return timer;
        }

        public void setTimer(TimeScheduler timer) {
            this.timer=timer;
        }

        public String getName() {
            return "ProtocolAdapter";
        }

        public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
        }

        public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
        }

        public String getInfo() {
            return null;
        }

        public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
        }

        public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
        }

        public void init() throws Exception {
            super.init();
        }

        public Object up(Event evt) {
            if(r != null)
                r.receive(evt);
            return null;
        }

        /** send to unicast or multicast destination */
        public Object down(Event evt) {
            try {
                send_queue.add(evt);
            }
            catch(QueueClosedException e) {
            }
            return null;
        }
    }



}
