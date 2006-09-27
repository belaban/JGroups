package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * Tests concurrent startup and message sending directly after joining
 * See doc/design/ConcurrentStartupTest.txt for details. This will only work 100% correctly once we have
 * FLUSH support (JGroups 2.4)
 * @author bela
 * @version $Id: ConcurrentStartupTest.java,v 1.13 2006/09/27 12:39:14 belaban Exp $
 */
public class ConcurrentStartupTest extends TestCase implements ExtendedReceiver {
    final List list=Collections.synchronizedList(new LinkedList());
    JChannel channel;
    final static String GROUP="demo";
    static String PROPS="flush-udp.xml"; // use flush properties
    final int NUM=5;
    int mod=1;
    final Map modifications=new TreeMap();


    protected void setUp() throws Exception {
        super.setUp();
        PROPS = System.getProperty("props",PROPS);
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
        channel.getState(null, 10000);
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
            thread.join(15000);
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
        synchronized(this) {
            list.add(obj);
            Integer key=new Integer(getMod());
            modifications.put(key, obj);
        }
    }

    public byte[] getState() {
        synchronized(this) {
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
            synchronized(this) {
                list.clear();
                list.addAll(tmp);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
    public byte[] getState(String state_id) {
    	//not needed
		return null;
	}

	public void getState(OutputStream ostream) {
		ObjectOutputStream oos = null;
        try{
           oos = new ObjectOutputStream(ostream);
           List tmp = null;
           synchronized (this){
              tmp = new LinkedList(list);
           }
           oos.writeObject(tmp);
           oos.flush();
        }
        catch (IOException e){
           e.printStackTrace();
        }
        finally{
           Util.closeOutputStream(oos);
        }

	}

	public void getState(String state_id, OutputStream ostream) {
		//not used

	}

	public void setState(String state_id, byte[] state) {
		// not used

	}

	public void setState(InputStream istream) {
		ObjectInputStream ois = null;
        try{
           ois = new ObjectInputStream(istream);
           List tmp = (List) ois.readObject();
           synchronized (this){
              list.clear();
              list.addAll(tmp);
           }
        }
        catch (Exception e){
           e.printStackTrace();
        }
        finally{
           Util.closeInputStream(ois);
        }
	}

	public void setState(String state_id, InputStream istream) {
		// not used

	}

    public void viewAccepted(View new_view) {
        System.out.println("-- view: " + new_view);
        synchronized(this) {
            Integer key=new Integer(getMod());
            modifications.put(key, new_view.getVid());
        }
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }


    private static class MyThread extends Thread {
        final List list=new LinkedList();
        Channel ch;
        int mod=1;
        final Map modifications=new TreeMap();


        int getMod() {
			int retval = mod;
			mod++;
			return retval;
		}


        MyThread(String name) {
            super(name);
        }

        public void run() {
            try {
                ch=new JChannel(PROPS);
                ch.setReceiver(new ExtendedReceiverAdapter() {
                    public void receive(Message msg) {
                        if(msg.getBuffer() == null)
                            return;
                        Object obj=msg.getObject();
                        synchronized (this) {
                        	list.add(obj);
                        	Integer key=new Integer(getMod());
                            modifications.put(key, obj);
						}
                    }

                    public void viewAccepted(View new_view) {
                        synchronized(this) {
                            Integer key=new Integer(getMod());
                            modifications.put(key, new_view.getVid());
                        }
                    }

                    public void setState(byte[] state) {
                        try {
                            List tmp=(List)Util.objectFromByteBuffer(state);
                            synchronized(this) {
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
						List tmp = null;
						synchronized (this) {
							tmp = new LinkedList(list);
							try {
								return Util.objectToByteBuffer(tmp);
							} catch (Exception e) {
								e.printStackTrace();
								return null;
							}
						}
					}

                    public void getState(OutputStream ostream){
                        ObjectOutputStream oos = null;
                        try{
                           oos = new ObjectOutputStream(ostream);
                           List tmp = null;
                           synchronized (this){
                              tmp = new LinkedList(list);
                           }
                           oos.writeObject(tmp);
                           oos.flush();
                        }
                        catch (IOException e){
                           e.printStackTrace();
                        }
                        finally{
                           Util.closeOutputStream(oos);
                        }
                    }

                    public void setState(InputStream istream) {
                       ObjectInputStream ois = null;
                       try{
                          ois = new ObjectInputStream(istream);
                          List tmp = (List) ois.readObject();
                          synchronized (this){
                             list.clear();
                             list.addAll(tmp);
                             System.out.println("-- [#" + getName() + " (" +ch.getLocalAddress()+")]: state is " + list);
                             Integer key=new Integer(getMod());
                             modifications.put(key, tmp);
                          }
                       }
                       catch (Exception e){
                          e.printStackTrace();
                       }
                       finally{
                          Util.closeInputStream(ois);
                       }
                    }
                });
                ch.connect(GROUP);
                ch.getState(null, 10000);
                // Util.sleep(1000);
                ch.send(null, null, ch.getLocalAddress());
                Util.sleep(10000); // potential retransmissions
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
