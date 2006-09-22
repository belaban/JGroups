
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.util.Promise;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Tests correct state transfer while other members continue sending messages to the group
 * @author Bela Ban
 * @version $Id: StateTransferTest.java,v 1.10 2006/09/22 12:33:12 belaban Exp $
 */
public class StateTransferTest extends TestCase {
    final int NUM=10000;
    final int NUM_THREADS=2;
    String props="udp.xml";


    public StateTransferTest(String name) {
        super(name);
    }
    
    protected void setUp() throws Exception {                    
       props = System.getProperty("props",props);   
       log("Using configuration file " + props);
       super.setUp();
    }


    public void testStateTransferWhileSending() throws Exception {
        Worker[] workers=new Worker[NUM_THREADS];

        int from=0, to=NUM;

        for(int i=0; i < workers.length; i++) {
            workers[i]=new Worker(from, to);
            from+=NUM;
            to+=NUM;
        }

        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            worker.start();
            Util.sleep(50); // to have threads join the group a bit later and get the state
        }

        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            worker.waitUntilDone();
        }
        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            worker.stop();
        }

        log("\n\nhashmaps\n");
        for(int i=0; i < workers.length; i++) {
            Worker w=workers[i];
            Map m=w.getMap();
            log("map has " + m.size() + " elements");
            assertEquals(NUM * NUM_THREADS, m.size());
        }

        Set keys=workers[0].getMap().keySet();
        for(int i=0; i < workers.length; i++) {
            Worker w=workers[i];
            Map m=w.getMap();
            Set s=m.keySet();
            assertEquals(keys, s);
        }
    }




    class Worker implements Runnable {
        JChannel        ch;
        int             to;
        int             from;
        final Promise   promise=new Promise();
        Thread          t;
        Receiver        receiver;


        public Worker(int from, int to) {
            this.to=to;
            this.from=from;
        }

        public Map getMap() {
            return receiver.getMap();
        }

        void start() throws Exception {
            ch=new JChannel(props);
            ch.connect("StateTransferTest-Group");
            receiver=new Receiver(ch, promise);
            boolean rc=ch.getState(null, 10000);
            if(rc)
                log("state transfer: OK");
            else {
                if(ch.getView().size() == 1)
                    log("state transfer: OK");
                else
                    log("state transfer: FAIL");
            }

            receiver.setName("Receiver [" + from + " - " + to + "]");
            receiver.start();
            if(rc)
                promise.getResult();

            t=new Thread(this);
            t.setName("Worker [" + from + " - " + to + "]");
            t.start();
        }

        public void stop() {
            ch.close();
        }

        void waitUntilDone() throws InterruptedException {
            t.join();
            receiver.join();
        }

        public void run() {
            Object[] data=new Object[2];
            log("Worker thread started (sending msgs from " + from + " to " + to + " (excluding " + to + ")");
            for(int i=from; i < to; i++) {
                data[0]=new Integer(i);
                data[1]="Value #" + i;
                try {
                    ch.send(null, null, data);
                    if(i % 1000 == 0)
                        log("sent " + i);
                    // log("sent " + data[0]);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    class Receiver extends Thread {
        JChannel ch;
        Promise promise;
        Map map;

        public Receiver(JChannel ch, Promise promise) {
            this.ch=ch;
            this.promise=promise;
            map=Collections.synchronizedMap(new HashMap(NUM * NUM_THREADS));
        }

        public Map getMap() {
            return map;
        }

        public void run() {
            Object obj, prev_val;
            Object[] data;
            int num_received=0, to_be_received=NUM * NUM_THREADS;

            log("Receiver thread started");
            while(ch.isConnected()) {
                try {
                    obj=ch.receive(0);
                    if(obj instanceof Message) {
                        data=(Object[])((Message)obj).getObject();
                        prev_val=map.put(data[0], data[1]);
                        if(prev_val != null) // we have a duplicate value
                            continue;
                        num_received=map.size();
                        if(num_received % 1000 == 0)
                            log("received " + num_received);

                        // log("received " + data[0] + " total: " + num_received + ")");

                        if(num_received >= to_be_received) {
                            log("DONE: received " + num_received + " messages");
                            break;
                        }
                    }
                    else if(obj instanceof View) {
                        log("VIEW: " + obj);
                    }
                    else if(obj instanceof GetStateEvent) {
                        byte[] state=Util.objectToByteBuffer(map);
                        log("returning state, map has " + map.size() + " elements");
                        ch.returnState(state);
                    }
                    else if(obj instanceof SetStateEvent) {
                        byte state[]=((SetStateEvent)obj).getArg();
                        if(state == null) {
                            log("received null state");
                        }
                        else {
                            Map tmp=(Map)Util.objectFromByteBuffer(state);
                            log("received state, map has " + tmp.size() + " elements");
                            map=Collections.synchronizedMap(tmp);
                        }
                        promise.setResult(Boolean.TRUE);
                    }
                    else if(obj instanceof StreamingGetStateEvent) {
                        StreamingGetStateEvent evt=(StreamingGetStateEvent)obj;
                        OutputStream stream = evt.getArg();
                        ObjectOutputStream out = new ObjectOutputStream(stream);
                        synchronized(map){
                           out.writeObject(map);
                        }
                        out.close();
                   }
                   else if(obj instanceof StreamingSetStateEvent) {
                        StreamingSetStateEvent evt=(StreamingSetStateEvent)obj;
                        InputStream stream = evt.getArg();
                        ObjectInputStream in = new ObjectInputStream(stream);
                        map=Collections.synchronizedMap((Map) in.readObject());
                        in.close();
                        promise.setResult(Boolean.TRUE);
                   }
                }
                catch(Exception e) {
                    log("receiver thread terminated due to exception: " + e);
                    break;
                }
            }
            log("Receiver thread terminated");
        }
    }


    static void log(String msg) {
        System.out.println(Thread.currentThread() + " -- "+ msg);
    }

    public static Test suite() {
        return new TestSuite(StateTransferTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }



}


