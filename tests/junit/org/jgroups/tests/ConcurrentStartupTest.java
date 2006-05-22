package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.*;

/**
 * Tests concurrent startup and message sending directly after joining
 * See doc/design/ConcurrentStartupTest.txt for details
 * @author bela
 * @version $Id: ConcurrentStartupTest.java,v 1.6 2006/05/22 07:10:59 belaban Exp $
 */
public class ConcurrentStartupTest extends TestCase implements Receiver {
    final List list=Collections.synchronizedList(new LinkedList());
    JChannel channel;
    final static String GROUP="demo";
    final static String PROPS="fc-fast-minimalthreads.xml"; // use default properties
    final int NUM=5;
    int mod=1;
    final Map modifications=new TreeMap();


    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }


    int getMod() {
        synchronized(this) {
            int retval=mod;
            mod++;
            return retval;
        }
    }


    public void testMessageSendingAfterConnect() throws Exception {
        channel=new JChannel(PROPS);
        channel.setReceiver(this);
        channel.connect(GROUP);
        channel.getState(null, 5000);
        // channel.send(null, null, channel.getLocalAddress());
        Util.sleep(2000);

        MyThread[] threads=new MyThread[NUM];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(String.valueOf(i));
            Util.sleepRandom(1000);
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

        Map[] mods=new Map[NUM];
        for(int i=0; i < threads.length; i++) {
            MyThread thread=threads[i];
            mods[i]=thread.getModifications();
        }

        printModifications(modifications, mods);


        int len=list.size();
        for(int i=0; i < lists.length; i++) {
            List l=lists[i];
            assertEquals("list #" + i + " should have " + len + " elements", len, l.size());
        }
    }

    private void printModifications(Map mod, Map[] modifications) {
        System.out.println("\nmodifications: " + mod);
        for(int i=0; i < modifications.length; i++) {
            Map modification=modifications[i];
            System.out.println("modifications for #" + i + ": " + modification);
        }
    }

    private void printLists(List list, List[] lists) {
        System.out.println("\nlist=" + list);
        for(int i=0; i < lists.length; i++) {
            List l=lists[i];
            System.out.println(i + ": " + l);
        }
    }


    public void receive(Message msg) {
        if(msg.getBuffer() == null)
            return;
        Object obj=msg.getObject();
        synchronized(list) {
            list.add(obj);
        }
        synchronized(modifications) {
            Integer key=new Integer(getMod());
            modifications.put(key, obj);
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
        System.out.println("-- view: " + new_view);
        synchronized(modifications) {
            Integer key=new Integer(getMod());
            modifications.put(key, new_view.getVid());
        }
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    private static class MyThread extends Thread {
        final List list=Collections.synchronizedList(new LinkedList());
        Channel ch;
        int mod=1;
        final Map modifications=new TreeMap();


        int getMod() {
            synchronized(this) {
                int retval=mod;
                mod++;
                return retval;
            }
        }


        MyThread(String name) {
            super(name);
        }

        public void run() {
            try {
                ch=new JChannel(PROPS);
                ch.setReceiver(new ReceiverAdapter() {
                    public void receive(Message msg) {
                        if(msg.getBuffer() == null)
                            return;
                        Object obj=msg.getObject();
                        list.add(obj);
                        synchronized(modifications) {
                            Integer key=new Integer(getMod());
                            modifications.put(key, obj);
                        }
                    }

                    public void viewAccepted(View new_view) {
                        synchronized(modifications) {
                            Integer key=new Integer(getMod());
                            modifications.put(key, new_view.getVid());
                        }
                    }

                    public void setState(byte[] state) {
                        try {
                            List tmp=(List)Util.objectFromByteBuffer(state);
                            synchronized(list) {
                                list.clear();
                                list.addAll(tmp);
                                System.out.println("-- [#" + getName() + " (" +ch.getLocalAddress()+")]: state is " + list);
                                Integer key=new Integer(getMod());
                                modifications.put(key, tmp);
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
                ch.getState(null, 5000);
                // Util.sleep(1000);
                ch.send(null, null, ch.getLocalAddress());
                Util.sleep(5000); // potential retransmissions
            }
            catch(ChannelException e) {
                e.printStackTrace();
            }
        }

        List getList() {return list;}
        Map getModifications() {return modifications;}
    }



    public static Test suite() {
        return new TestSuite(ConcurrentStartupTest.class);
    }

    public static void main(String[] args) {
        String[] testCaseName={ConcurrentStartupTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
