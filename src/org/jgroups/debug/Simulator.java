package org.jgroups.debug;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Tests one or more protocols independently. Look at org.jgroups.tests.FCTest for an example of how to use it.
 * @author Bela Ban
 * @version $Id: Simulator.java,v 1.1 2004/09/23 16:29:16 belaban Exp $
 */
public class Simulator {
    private Protocol[] protStack=null;
    private ProtocolAdapter ad=new ProtocolAdapter();
    private Receiver r=null;
    private Protocol top=null, bottom=null;
    private Queue send_queue=new Queue();
    private Thread send_thread;
    private Queue recv_queue=new Queue();
    private Thread recv_thread;


    /** HashMap<Address,Simulator> */
    public static HashMap addrTable=new HashMap();
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

        if(protStack.length > 1) {
            for(int i=0; i < protStack.length; i++) {
                Protocol p1=protStack[i];
                Protocol p2=i+1 >= protStack.length? null : protStack[i+1];
                if(p2 != null) {
                    p1.setDownProtocol(p2);
                    p2.setUpProtocol(p1);
                }
            }
        }
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

    public void send(Event evt) {
        top.down(evt);
    }

    public void receive(Event evt) {
        try {
            recv_queue.add(evt);
        }
        catch(QueueClosedException e) {
        }
    }

    public void start() throws Exception {
       if(local_addr == null)
            throw new Exception("local_addr has to be non-null");
        if(protStack == null)
            throw new Exception("protocol stack is null");

        bottom.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        if(view != null)
            top.down(new Event(Event.VIEW_CHANGE, view));


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
    }




    class ProtocolAdapter extends Protocol {

        public String getName() {
            return "ProtocolAdapter";
        }

        public void up(Event evt) {
            if(r != null)
                r.receive(evt);
        }

        /** send to unicast or multicast destination */
        public void down(Event evt) {
            try {
                send_queue.add(evt);
            }
            catch(QueueClosedException e) {
            }
        }
    }



}
