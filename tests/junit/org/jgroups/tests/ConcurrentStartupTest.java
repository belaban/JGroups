package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests concurrent startup and message sending directly after joining
 * @author bela
 * @version $Id: ConcurrentStartupTest.java,v 1.1 2006/05/19 21:13:04 belaban Exp $
 */
public class ConcurrentStartupTest extends TestCase implements Receiver {
    final List list=Collections.synchronizedList(new LinkedList());
    JChannel channel;
    final static String GROUP="demo";
    final static String PROPS=null; // use default properties
    final int NUM=5;


    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testMessageSendingAfterConnect() throws Exception {
        channel=new JChannel(PROPS);
        channel.setReceiver(this);
        channel.connect(GROUP);
        channel.getState(null, 5000);
        channel.send(null, null, channel.getLocalAddress());
        Util.sleep(2000);

        MyThread[] threads=new MyThread[NUM];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(String.valueOf(i));
            threads[i].start();
        }

        for(int i=0; i < threads.length; i++) {
            MyThread thread=threads[i];
            thread.join(10000);
            if(thread.isAlive())
                System.err.println("thread " + i + " is still alive");
        }

        List[] lists=new List[NUM];
        for(int i=0; i < threads.length; i++) {
            MyThread thread=threads[i];
            lists[i]=new LinkedList(thread.getList());
        }

        printLists(list, lists);


        int len=list.size();
        for(int i=0; i < lists.length; i++) {
            List l=lists[i];
            assertEquals("list #" + i + " should have " + len + " elements", len, l.size());
        }
    }

    private void printLists(List list, List[] lists) {
        System.out.println("list=" + list);
        for(int i=0; i < lists.length; i++) {
            List l=lists[i];
            System.out.println(i + ": " + l);
        }
    }


    public void receive(Message msg) {
        Object obj=msg.getObject();
        synchronized(list) {
            list.add(obj);
        }
    }

    public byte[] getState() {
        synchronized(list) {
            List tmp=new LinkedList(list);
            try {
                return Util.objectToByteBuffer(tmp);
            }
            catch(Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public void setState(byte[] state) {
        try {
            List tmp=(List)Util.objectFromByteBuffer(state);
            synchronized(list) {
                list.clear();
                list.addAll(tmp);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void viewAccepted(View new_view) {
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    private static class MyThread extends Thread {
        final List list=Collections.synchronizedList(new LinkedList());
        Channel ch;

        MyThread(String name) {
            super(name);
        }

        public void run() {
            try {
                ch=new JChannel(PROPS);
                ch.setReceiver(new ReceiverAdapter() {
                    public void receive(Message msg) {
                        Object obj=msg.getObject();
                        list.add(obj);
                    }

                    public void setState(byte[] state) {
                        try {
                            List tmp=(List)Util.objectFromByteBuffer(state);
                            synchronized(list) {
                                list.clear();
                                list.addAll(tmp);
                            }
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                    }

                    public byte[] getState() {
                        synchronized(list) {
                            List tmp=new LinkedList(list);
                            try {
                                return Util.objectToByteBuffer(tmp);
                            }
                            catch(Exception e) {
                                e.printStackTrace();
                                return null;
                            }
                        }
                    }
                });
                ch.connect(GROUP);
                ch.send(null, null, ch.getLocalAddress());
            }
            catch(ChannelException e) {
                e.printStackTrace();
            }
        }

        List getList() {return list;}
    }



    public static Test suite() {
        return new TestSuite(ConcurrentStartupTest.class);
    }

    public static void main(String[] args) {
        String[] testCaseName={ConcurrentStartupTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
