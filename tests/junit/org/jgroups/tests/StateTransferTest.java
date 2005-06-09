
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.HashMap;
import java.util.Set;

import org.jgroups.*;
import org.jgroups.util.Util;


/**
 * Tests correct state transfer while other members continue sending messages to the group
 * @author Bela Ban
 * @version $Id: StateTransferTest.java,v 1.3 2005/06/09 09:05:15 belaban Exp $
 */
public class StateTransferTest extends TestCase {
    final int NUM=10000;
    final int NUM_THREADS=2;
    final String props="fc-fast.xml";



    public StateTransferTest(String name) {
        super(name);
    }


    public void testStateTransferWhileSending() throws Exception {
        Worker[] workers=new Worker[NUM_THREADS];
        HashMap[] maps=new HashMap[NUM_THREADS];
        int start=0;

        for(int i=0; i < maps.length; i++) {
            maps[i]=new HashMap(NUM_THREADS * NUM);
        }

        for(int i=0; i < workers.length; i++) {
            workers[i]=new Worker(i, maps[i], start);
            start+=NUM;
        }

        for(int i=0; i < workers.length; i++) {
            Worker worker=workers[i];
            Util.sleep(1000);
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

        log("\n\nhashmaps:\n");
        for(int i=0; i < maps.length; i++) {
            HashMap map=maps[i];
            log("#" + (i+1) + ": " + map.size() + " elements");
        }

        int size=maps[0].size();
        for(int i=0; i < maps.length; i++) {
            HashMap map=maps[i];
            assertEquals(size, map.size());
        }

        Set keys=maps[0].keySet();
        for(int i=0; i < maps.length; i++) {
            HashMap map=maps[i];
            assertTrue(map.keySet().containsAll(keys));
        }
    }




    class Worker implements Runnable {
        HashMap  m;
        JChannel ch;
        Thread   t, receiver;
        int      start_num=0;
        int      id;


        public Worker(int id, HashMap m, int start_num) {
            this.m=m;
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

            receiver=new Receiver(m, ch);
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
        HashMap m;
        JChannel ch;

        public Receiver(HashMap m, JChannel ch) {
            this.m=m;
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
                        m.put(data[0], data[1]);
                        num_received++;
                        if(num_received % 1000 == 0)
                            log("received " + num_received);
                        if(m.size() >= to_be_received) {
                            log("DONE: received " + m.size() + " messages");
                            break;
                        }
                    }
                    else if(obj instanceof View) {
                        log("VIEW: " + obj);
                    }
                    else if(obj instanceof GetStateEvent) {
                        byte[] state=Util.objectToByteBuffer(m);
                        log("returning state, map has " + m.size() + " elements");
                        ch.returnState(state);
                    }
                    else if(obj instanceof SetStateEvent) {
                        byte state[]=((SetStateEvent)obj).getArg();
                        if(state == null) {
                            log("received null state");
                        }
                        else {
                            HashMap tmp=(HashMap)Util.objectFromByteBuffer(state);
                            log("received state, map has " + tmp.size() + " elements");
                            m=tmp;
                        }
                    }
                }
                catch(Exception e) {
                    e.printStackTrace();
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


