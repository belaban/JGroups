package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.HashMap;
import java.util.Map;

/**
 * Test the multiplexer functionality provided by JChannelFactory
 * @author Bela Ban
 * @version $Id: MultiplexerTest.java,v 1.1 2006/03/17 12:09:50 belaban Exp $
 */
public class MultiplexerTest extends TestCase {
    private Cache c1, c2, c1_repl, c2_repl;
    private Channel ch1, ch2, ch1_repl, ch2_repl;
    static final String CFG="stacks.xml";
    static final String STACK_NAME="fc-fast-minimalthreads";
    JChannelFactory factory, factory2;

    public MultiplexerTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(CFG);

        factory2=new JChannelFactory();
        factory2.setMultiplexerConfig(CFG);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(ch1_repl != null)
            ch1_repl.close();
        if(ch2_repl != null)
            ch2_repl.close();
        if(ch1 != null)
            ch1.close();
        if(ch2 != null)
            ch2.close();
    }


    public void testReplicationWithOneChannel() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());
        c1.put("name", "Bela");
        Util.sleep(300); // we need to wait because replication is asynchronous here
        assertEquals(1, c1.size());
        assertEquals("Bela", c1.get("name"));
    }


    public void testReplicationWithTwoChannels() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1");
        ch1_repl.connect("bla");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        assertEquals("cache has to be empty initially", 0, c1_repl.size());

        c1.put("name", "Bela");
        Util.sleep(500);

        System.out.println("c1: " + c1 + ", c1_repl: " + c1_repl);

        assertEquals(1, c1.size());
        assertEquals("Bela", c1.get("name"));

        assertEquals(1, c1_repl.size());
        assertEquals("Bela", c1_repl.get("name"));

        c1.put("id", new Long(322649));
        c1_repl.put("hobbies", "biking");
        c1_repl.put("bike", "Centurion");

        Util.sleep(500);
        System.out.println("c1: " + c1 + ", c1_repl: " + c1_repl);
        
        assertEquals(4, c1.size());
        assertEquals(4, c1_repl.size());

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));
    }


    public static Test suite() {
        return new TestSuite(MultiplexerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(MultiplexerTest.suite());
    }


    private static class Cache extends ExtendedReceiverAdapter {
        Map data=new HashMap();
        Channel ch;
        String name;

        public Cache(Channel ch, String name) {
            this.ch=ch;
            this.name=name;
            this.ch.setReceiver(this);
        }

        Object get(Object key) {
            synchronized(this) {
                return data.get(key);
            }
        }

        void put(Object key, Object val) throws Exception {
            Object[] buf=new Object[2];
            buf[0]=key; buf[1]=val;
            Message msg=new Message(null, null, buf);
            ch.send(msg);
        }

        int size() {
            synchronized(this) {
                return data.size();
            }
        }


        public void receive(Message msg) {
            Object[] modification=(Object[])msg.getObject();
            Object key=modification[0];
            Object val=modification[1];
            synchronized(this) {
                data.put(key,val);
            }
        }

        public byte[] getState() {
            byte[] state=null;
            synchronized(this) {
                try {
                    state=Util.objectToByteBuffer(data);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
            return state;
        }

        public void setState(byte[] state) {
            Map m;
            try {
                m=(Map)Util.objectFromByteBuffer(state);
                synchronized(this) {
                    data.clear();
                    data=m;
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }

        }

        public void viewAccepted(View new_view) {
            log("view is " + new_view);
        }

        public String toString() {
            return data.toString();
        }

        private void log(String msg) {
            System.out.println("-- [" + name + "] " + msg);
        }

    }
}
