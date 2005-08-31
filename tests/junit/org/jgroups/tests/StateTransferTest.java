
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Tests correct state transfer while other members continue sending messages to the group
 * @author Bela Ban
 * @version $Id: StateTransferTest.java,v 1.4 2005/08/31 08:35:00 belaban Exp $
 */
public class StateTransferTest extends TestCase {
    final int NUM=10000;
    final int NUM_THREADS=2;
    final String props="fc-fast.xml";
    Worker[] workers;
    Map map;


    public StateTransferTest(String name) {
        super(name);
    }


    public void testStateTransferWhileSending() throws Exception {
        workers=new Worker[NUM_THREADS];
        map=Collections.synchronizedMap(new HashMap(NUM * NUM_THREADS));
        int start=0;

        for(int i=0; i < workers.length; i++) {
            workers[i]=new Worker(i, start);
            start+=NUM;
        }

        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            Util.sleep(50); // to have threads join the group a bit later and get the state
            worker.start();
        }

        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            worker.waitUntilDone();
        }
        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            worker.stop();
        }

        log("\n\nhashmap has " + map.size() + " elements");
        assertEquals(NUM * NUM_THREADS, map.size());
    }




    class Worker implements Runnable {
        JChannel ch;
        Thread   t, receiver;
        int      start_num=0;
        int      id;


        public Worker(int id, int start_num) {
            this.start_num=start_num;
            this.id=id;
        }

        void start() throws Exception {
            ch=new JChannel(props);
            ch.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            ch.connect("StateTransferTest-Group");
            boolean rc=ch.getState(null, 10000);
            if(rc)
                log("state transfer: OK");
            else {
                if(ch.getView().size() == 1)
                    log("state transfer: OK");
                else
                    log("state transfer: FAIL");
            }

            receiver=new Receiver(id, ch);
            receiver.setName("Receiver #" + id);
            receiver.start();

            t=new Thread(this, "worker #" + id);
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
            log("Worker thread started");
            for(int i=start_num; i < start_num + NUM; i++) {
                data[0]=new Integer(i);
                data[1]="Value #" + i;
                try {
                    ch.send(null, null, data);
                    if(i % 1000 == 0)
                    log("sent " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    class Receiver extends Thread {
        int index;
        JChannel ch;

        public Receiver(int index, JChannel ch) {
            this.index=index;
            this.ch=ch;
        }

        public void run() {
            Object obj;
            Object[] data;
            int num_received=0, to_be_received=NUM * NUM_THREADS;

            log("Receiver thread started");
            while(ch.isConnected()) {
                try {
                    obj=ch.receive(0);
                    if(obj instanceof Message) {
                        data=(Object[])((Message)obj).getObject();
                        map.put(data[0], data[1]);
                        num_received=map.size();
                        if(num_received % 1000 == 0)
                            log("received " + num_received);
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
                            map=tmp;
                        }
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
        TestSuite s=new TestSuite(StateTransferTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }



}


