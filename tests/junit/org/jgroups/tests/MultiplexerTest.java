package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.*;

/**
 * Test the multiplexer functionality provided by JChannelFactory
 * @author Bela Ban
 * @version $Id: MultiplexerTest.java,v 1.8 2006/07/27 09:58:15 belaban Exp $
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

        if(c1 != null) c1.clear();
        if(c2 != null) c2.clear();
        if(c1_repl != null) c1_repl.clear();
        if(c2_repl != null) c2_repl.clear();
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

    public void testReplicationWithReconnect() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());
        c1.put("name", "Bela");
        Util.sleep(300); // we need to wait because replication is asynchronous here
        assertEquals(1, c1.size());
        assertEquals("Bela", c1.get("name"));

        ch1.disconnect();

        ch1.connect("bla");

        c2=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c2.size());
        c2.put("name", "Bela");
        Util.sleep(300); // we need to wait because replication is asynchronous here
        assertEquals(1, c2.size());
        assertEquals("Bela", c2.get("name"));

    }


    public void testStateTransfer() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1");

        c1.put("name", "Bela");
        c1.put("id", new Long(322649));
        c1.put("hobbies", "biking");
        c1.put("bike", "Centurion");


        ch1_repl.connect("bla");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("c1_repl: " + c1_repl);
        assertEquals("initial state should have been transferred", 4, c1_repl.size());

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));
    }


    public void testStateTransferWithTwoApplications() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch2=factory.createMultiplexerChannel(STACK_NAME, "c2");
        ch2.connect("bla");
        c2=new Cache(ch2, "cache-2");
        assertEquals("cache has to be empty initially", 0, c2.size());

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1");

        ch2_repl=factory2.createMultiplexerChannel(STACK_NAME, "c2");


        c1.put("name", "cache-1");
        c2.put("name", "cache-2");

        ch1_repl.connect("bla");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);

        ch2_repl.connect("bla");
        c2_repl=new Cache(ch2_repl, "cache-2-repl");
        rc=ch2_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c1_repl: " + c1_repl);
        System.out.println("c2: " + c2);
        System.out.println("c2_repl: " + c2_repl);

        assertEquals(1, c1.size());
        assertEquals(1, c1_repl.size());

        assertEquals(1, c2.size());
        assertEquals(1, c2_repl.size());

        assertEquals("cache-1", c1.get("name"));
        assertEquals("cache-1", c1_repl.get("name"));

        assertEquals("cache-2", c2.get("name"));
        assertEquals("cache-2", c2_repl.get("name"));
    }


    public void testStateTransferWithRegistration() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch2=factory.createMultiplexerChannel(STACK_NAME, "c2");
        ch2.connect("bla");
        c2=new Cache(ch2, "cache-2");
        assertEquals("cache has to be empty initially", 0, c2.size());
        c1.put("name", "cache-1");
        c2.put("name", "cache-2");

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1", true, null); // register for state transfer
        ch2_repl=factory2.createMultiplexerChannel(STACK_NAME, "c2", true, null); // register for state transfer

        ch1_repl.connect("bla");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, 5000); // this will *not* trigger the state transfer protocol
        System.out.println("state transfer: " + rc);

        ch2_repl.connect("bla");
        c2_repl=new Cache(ch2_repl, "cache-2-repl");
        rc=ch2_repl.getState(null, 5000); // only *this* will trigger the state transfer
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c1_repl: " + c1_repl);
        System.out.println("c2: " + c2);
        System.out.println("c2_repl: " + c2_repl);

        assertEquals(1, c1.size());
        assertEquals(1, c1_repl.size());

        assertEquals(1, c2.size());
        assertEquals(1, c2_repl.size());

        assertEquals("cache-1", c1.get("name"));
        assertEquals("cache-1", c1_repl.get("name"));

        assertEquals("cache-2", c2.get("name"));
        assertEquals("cache-2", c2_repl.get("name"));
        c1.clear();
        c1_repl.clear();
        c2.clear();
        c2_repl.clear();
    }


    public void testStateTransferWithReconnect() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1");

        c1.put("name", "Bela");
        c1.put("id", new Long(322649));
        c1.put("hobbies", "biking");
        c1.put("bike", "Centurion");


        ch1_repl.connect("bla");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("c1_repl: " + c1_repl);
        assertEquals("initial state should have been transferred", 4, c1_repl.size());

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));

        ch1_repl.disconnect();

        ch1_repl.connect("bla");
        c2_repl=new Cache(ch1_repl, "cache-2-repl");
        rc=ch1_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("c2_repl: " + c2_repl);
        assertEquals("initial state should have been transferred", 4, c2_repl.size());

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c2_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c2_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c2_repl.get("bike"));
    }


    public void testGetSubstates() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new ExtendedCache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch2=factory.createMultiplexerChannel(STACK_NAME, "c2");
        ch2.connect("bla");
        c2=new ExtendedCache(ch2, "cache-2");
        assertEquals("cache has to be empty initially", 0, c2.size());

        for(int i=0; i < 10; i++) {
            c1.put(new Integer(i), new Integer(i));
            c2.put(new Integer(i), new Integer(i));
        }

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1");
        ch2_repl=factory2.createMultiplexerChannel(STACK_NAME, "c2");
        ch1_repl.connect("bla");
        c1_repl=new ExtendedCache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, "odd", 5000);
        System.out.println("state transfer: " + rc);

        ch2_repl.connect("bla");
        c2_repl=new ExtendedCache(ch2_repl, "cache-2-repl");
        rc=ch2_repl.getState(null, "even", 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c2: " + c2);

        System.out.println("c1_repl (removed odd substate): " + c1_repl);
        System.out.println("c2_repl (removed even substate): " + c2_repl);

        assertEquals(5, c1_repl.size());
        assertEquals(5, c2_repl.size());

        _testEvenNumbersPresent(c1_repl);
        _testOddNumbersPresent(c2_repl);
    }

    private void _testEvenNumbersPresent(Cache c) {
        Integer[] evens=new Integer[]{new Integer(0), new Integer(2), new Integer(4), new Integer(6), new Integer(8)};
        _testNumbersPresent(c, evens);

    }

    private void _testOddNumbersPresent(Cache c) {
        Integer[] odds=new Integer[]{new Integer(1), new Integer(3), new Integer(5), new Integer(7), new Integer(9)};
        _testNumbersPresent(c, odds);
    }

    private void _testNumbersPresent(Cache c, Integer[] numbers) {
        int len=numbers.length;
        assertEquals(len, c.size());
        for(int i=0; i < numbers.length; i++) {
            Integer number=numbers[i];
            assertEquals(number, c.get(number));
        }
    }



    public void testGetSubstatesMultipleTimes() throws Exception {
        ch1=factory.createMultiplexerChannel(STACK_NAME, "c1");
        ch1.connect("bla");
        c1=new ExtendedCache(ch1, "cache-1");
        assertEquals("cache has to be empty initially", 0, c1.size());

        ch2=factory.createMultiplexerChannel(STACK_NAME, "c2");
        ch2.connect("bla");
        c2=new ExtendedCache(ch2, "cache-2");
        assertEquals("cache has to be empty initially", 0, c2.size());

        for(int i=0; i < 10; i++) {
            c1.put(new Integer(i), new Integer(i));
            c2.put(new Integer(i), new Integer(i));
        }

        ch1_repl=factory2.createMultiplexerChannel(STACK_NAME, "c1");
        ch2_repl=factory2.createMultiplexerChannel(STACK_NAME, "c2");
        ch1_repl.connect("bla");
        c1_repl=new ExtendedCache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, "odd", 5000);
        System.out.println("state transfer: " + rc);

        ch2_repl.connect("bla");
        c2_repl=new ExtendedCache(ch2_repl, "cache-2-repl");
        rc=ch2_repl.getState(null, "even", 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);
        _testOddNumbersPresent(c2_repl);

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c2: " + c2);
        System.out.println("c1_repl (removed odd substate): " + c1_repl);
        System.out.println("c2_repl (removed even substate): " + c2_repl);

        assertEquals(5, c2_repl.size());
        rc=ch2_repl.getState(null, "odd", 5000);
        Util.sleep(500);
        System.out.println("c2_repl (removed odd substate): " + c2_repl);
        _testEvenNumbersPresent(c2_repl);

        assertEquals(5, c2_repl.size());
        rc=ch2_repl.getState(null, "even", 5000);
        Util.sleep(500);
        System.out.println("c2_repl (removed even substate): " + c2_repl);
        _testOddNumbersPresent(c2_repl);

        assertEquals(5, c2_repl.size());
        rc=ch2_repl.getState(null, "odd", 5000);
        Util.sleep(500);
        System.out.println("c2_repl (removed odd substate): " + c2_repl);
        _testEvenNumbersPresent(c2_repl);
    }



    public static Test suite() {
        return new TestSuite(MultiplexerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(MultiplexerTest.suite());
    }


    private static class Cache extends ExtendedReceiverAdapter {
        final Map data=new HashMap();
        Channel ch;
        String name;

        public Cache(Channel ch, String name) {
            this.ch=ch;
            this.name=name;
            this.ch.setReceiver(this);
        }

        protected Object get(Object key) {
            synchronized(data) {
                return data.get(key);
            }
        }

        protected  void put(Object key, Object val) throws Exception {
            Object[] buf=new Object[2];
            buf[0]=key; buf[1]=val;
            Message msg=new Message(null, null, buf);
            ch.send(msg);
        }

        protected int size() {
            synchronized(data) {
                return data.size();
            }
        }


        public void receive(Message msg) {
            Object[] modification=(Object[])msg.getObject();
            Object key=modification[0];
            Object val=modification[1];
            synchronized(data) {
                data.put(key,val);
            }
        }

        public byte[] getState() {
            byte[] state=null;
            synchronized(data) {
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

        public byte[] getState(String state_id) {
            return getState();
        }


        public void setState(byte[] state) {
            Map m;
            try {
                m=(Map)Util.objectFromByteBuffer(state);
                synchronized(data) {
                    data.clear();
                    data.putAll(m);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void setState(String state_id, byte[] state) {
            setState(state);
        }


        public void clear() {
            data.clear();
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


    static class ExtendedCache extends Cache {

        public ExtendedCache(Channel ch, String name) {
            super(ch, name);
        }


        public byte[] getState(String state_id) {
            Map copy=new HashMap(data);
            for(Iterator it=copy.keySet().iterator(); it.hasNext();) {
                Integer key=(Integer)it.next();
                if(state_id.equals("odd") && key.intValue() % 2 != 0)
                    it.remove();
                else if(state_id.equals("even") && key.intValue() % 2 == 0)
                    it.remove();
            }
            try {
                return Util.objectToByteBuffer(copy);
            }
            catch(Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        public void setState(String state_id, byte[] state) {
            setState(state);
        }

        public String toString() {
            synchronized(data) {
                Set keys=new TreeSet(data.keySet());
                StringBuffer sb=new StringBuffer();
                for(Iterator it=keys.iterator(); it.hasNext();) {
                    Object o=it.next();
                    sb.append(o).append("=").append(data.get(o)).append(" ");
                }
                return sb.toString();
            }
        }
    }

}
