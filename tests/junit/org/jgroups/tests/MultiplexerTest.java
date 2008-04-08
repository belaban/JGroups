package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.mux.MuxChannel;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.*;
import java.util.*;

/**
 * Test the multiplexer functionality provided by JChannelFactory
 * @author Bela Ban
 * @version $Id: MultiplexerTest.java,v 1.49 2008/04/08 07:19:00 belaban Exp $
 */
public class MultiplexerTest extends ChannelTestBase {
    private Cache c1, c2, c1_repl, c2_repl;
    private Channel ch1, ch2, ch1_repl, ch2_repl;
    JChannelFactory factory, factory2;   

    public MultiplexerTest(String name) {
        super(name);
    }


    @BeforeMethod
    public void setUp() throws Exception {
        ;
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(mux_conf);

        factory2=new JChannelFactory();
        factory2.setMultiplexerConfig(mux_conf);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if(ch1_repl != null)
            ch1_repl.close();
        if(ch2_repl != null)
            ch2_repl.close();
        if(ch1 != null)
            ch1.close();
        if(ch2 != null)
            ch2.close();
        if(ch1 != null) {
            assertFalse(((MuxChannel)ch1).getChannel().isOpen());
            assertFalse(((MuxChannel)ch1).getChannel().isConnected());
        }
        if(ch2 != null) {
            assertFalse(((MuxChannel)ch2).getChannel().isOpen());
            assertFalse(((MuxChannel)ch2).getChannel().isConnected());
        }
        if(ch1_repl != null) {
            assertFalse(((MuxChannel)ch1_repl).getChannel().isOpen());
            assertFalse(((MuxChannel)ch1_repl).getChannel().isConnected());
        }
        if(ch2_repl != null) {
            assertFalse(((MuxChannel)ch2_repl).getChannel().isOpen());
            assertFalse(((MuxChannel)ch2_repl).getChannel().isConnected());
        }

        if(c1 != null) c1.clear();
        if(c2 != null) c2.clear();
        if(c1_repl != null) c1_repl.clear();
        if(c2_repl != null) c2_repl.clear();

        ch1_repl=ch2_repl=ch1=ch2=null;
        c1=c2=c1_repl=c2_repl=null; 
        
        ;
    }


    @org.testng.annotations.Test
    public void testReplicationWithOneChannel() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");
        c1.put("name", "Bela");
        Util.sleep(300); // we need to wait because replication is asynchronous here
        Assert.assertEquals(1, c1.size());
        assertEquals("Bela", c1.get("name"));
    }


    @org.testng.annotations.Test
    public void testLifecycle() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        assertTrue(ch1.isOpen());
        assertFalse(ch1.isConnected());

        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch2.connect("bla");
        assertTrue(ch2.isOpen());
        assertTrue(ch2.isConnected());

        ch2.disconnect();
        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch2.connect("bla");
        assertTrue(ch2.isOpen());
        assertTrue(ch2.isConnected());

        ch2.disconnect();
        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch2.close();
        assertFalse(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        ch2.connect("bla");
        assertTrue(ch2.isOpen());
        assertTrue(ch2.isConnected());

        ch2.close();
        assertFalse(ch2.isOpen());
        assertFalse(ch2.isConnected());
    }


    @org.testng.annotations.Test
    public void testDisconnect() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        assertTrue(ch1.isOpen());
        assertFalse(ch1.isConnected());
        assertTrue(((MuxChannel)ch1).getChannel().isOpen());
        assertFalse(((MuxChannel)ch1).getChannel().isConnected());

        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());
        assertTrue(((MuxChannel)ch1).getChannel().isOpen());
        assertTrue(((MuxChannel)ch1).getChannel().isConnected());

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch1.disconnect();
        assertTrue(ch1.isOpen());
        assertFalse(ch1.isConnected());

        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());

        ch1.close();
        assertFalse(ch1.isOpen());
        assertFalse(ch1.isConnected());
        assertTrue(((MuxChannel)ch1).getChannel().isOpen());
        assertTrue(((MuxChannel)ch1).getChannel().isConnected());

        ch2.close();
        assertFalse(ch2.isOpen());
        assertFalse(ch2.isConnected());
    }

    @org.testng.annotations.Test
    public void testDisconnect2() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        assertTrue(ch1.isOpen());
        assertFalse(ch1.isConnected());

        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch1.disconnect();
        assertTrue(ch1.isOpen());
        assertFalse(ch1.isConnected());

        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());

        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());

        assertTrue(ch2.isOpen());
        assertFalse(ch2.isConnected());
    }


    @org.testng.annotations.Test
    public void testClose() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        ch2.connect("bla");
        ch1.close();
        ch2.close();
    }


    @org.testng.annotations.Test
    public void testReplicationWithTwoChannels() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        c1=new Cache(ch1, "cache-1");
        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");
        ch1.connect("bla");

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        Assert.assertEquals(c1_repl.size(), 0, "cache has to be empty initially");
        ch1_repl.connect("bla");
        Util.sleep(200);

        View v=ch1_repl.getView();
        assertNotNull(v);
        Assert.assertEquals(v.size(), 2, "view is " + v);
        v=ch1.getView();
        assertNotNull(v);
        System.out.println("checking view " + v);
        Assert.assertEquals(2, v.size());

        // System.out.println("****** [c1] PUT(name, Bela) *******");
        c1.put("name", "Bela");
        if(ch1.flushSupported()) {
            boolean success=ch1.startFlush(true);
            System.out.println("startFlush(): " + success);
            assertTrue(success);
        }
        else
            Util.sleep(10000);

        System.out.println("c1: " + c1 + ", c1_repl: " + c1_repl);

        Assert.assertEquals(1, c1.size());
        assertEquals("Bela", c1.get("name"));

        Util.sleep(500); // async repl - wait until replicated to other member
        Assert.assertEquals(1, c1_repl.size());
        assertEquals("Bela", c1_repl.get("name"));

        c1.put("id", new Long(322649));
        c1_repl.put("hobbies", "biking");
        c1_repl.put("bike", "Centurion");
         if(ch1.flushSupported()) {
             boolean success=ch1.startFlush(true);
             System.out.println("startFlush(): " + success);
             assertTrue(success);
         }
        else
            Util.sleep(10000);

        System.out.println("c1: " + c1 + ", c1_repl: " + c1_repl);

        Assert.assertEquals(c1.size(), 4, "c1: " + c1);
        Assert.assertEquals(c1_repl.size(), 4, "c1_repl: " + c1_repl);

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));
    }


    @org.testng.annotations.Test
    public void testVirtualSynchrony() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        c1=new Cache(ch1, "cache-1");
        ch1.connect("bla");

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        ch1_repl.connect("bla");
        Assert.assertEquals(ch1.getView().size(), 2, "view: " + ch1.getView());

        // start adding messages
        flush(ch1, 5000); // flush all pending message out of the system so everyone receives them

        for(int i=1; i <= 20; i++) {
            if(i % 2 == 0) {
                c1.put(i, Boolean.TRUE); // even numbers
            }
            else {
                c1_repl.put(i, Boolean.TRUE); // odd numbers
            }
        }

        flush(ch1, 5000);
        System.out.println("c1 (" + c1.size() + " elements):\n" + c1.printKeys() +
                "\nc1_repl (" + c1_repl.size() + " elements):\n" + c1_repl.printKeys());
        Assert.assertEquals(c1.size(), c1_repl.size());
        Assert.assertEquals(20, c1.size());
    }


    private static void flush(Channel channel, long timeout) {
        if(channel.flushSupported()) {
            boolean success=channel.startFlush(true);
            System.out.println("startFlush(): " + success);
            assertTrue(success);
        }
        else
            Util.sleep(timeout);
    }

    @org.testng.annotations.Test
    public void testReplicationWithReconnect() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");
        c1.put("name", "Bela");
        Util.sleep(300); // we need to wait because replication is asynchronous here
        Assert.assertEquals(1, c1.size());
        assertEquals("Bela", c1.get("name"));

        ch1.disconnect();

        ch1.connect("bla");

        c2=new Cache(ch1, "cache-1");
        Assert.assertEquals(c2.size(), 0, "cache has to be empty initially");
        c2.put("name", "Bela");
        Util.sleep(300); // we need to wait because replication is asynchronous here
        Assert.assertEquals(1, c2.size());
        assertEquals("Bela", c2.get("name"));

    }
    
    @org.testng.annotations.Test
    public void testAdditionalData() throws Exception {
        byte[] additional_data=new byte[]{'b', 'e', 'l', 'a'};
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        Map<String,Object> m=new HashMap<String,Object>(1);
        m.put("additional_data", additional_data);
        ch1.down(new Event(Event.CONFIG, m));
        ch1.connect("bla");
        IpAddress local_addr=(IpAddress)ch1.getLocalAddress();
        assertNotNull(local_addr);
        byte[] tmp=local_addr.getAdditionalData();
        assertNotNull(tmp);
        assertEquals(tmp, additional_data);

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        ch2.connect("foo");
        local_addr=(IpAddress)ch2.getLocalAddress();
        assertNotNull(local_addr);
        tmp=local_addr.getAdditionalData();
        assertNotNull(tmp);
        assertEquals(tmp, additional_data);
    }

    @org.testng.annotations.Test
    public void testAdditionalData2() throws Exception {
        byte[] additional_data=new byte[]{'b', 'e', 'l', 'a'};
        byte[] additional_data2=new byte[]{'m', 'i', 'c', 'h', 'i'};
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        IpAddress local_addr=(IpAddress)ch1.getLocalAddress();
        assertNotNull(local_addr);
        byte[] tmp=local_addr.getAdditionalData();
        assert tmp == null;

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        Map<String,Object> m=new HashMap<String,Object>(1);
        m.put("additional_data", additional_data);
        ch2.down(new Event(Event.CONFIG, m));
        ch2.connect("foo");
        local_addr=(IpAddress)ch2.getLocalAddress();
        assertNotNull(local_addr);
        tmp=local_addr.getAdditionalData();
        assertNotNull(tmp);
        assertEquals(tmp, additional_data);

        local_addr=(IpAddress)ch1.getLocalAddress();
        assertNotNull(local_addr);
        tmp=local_addr.getAdditionalData();
        assertNotNull(tmp);
        assertEquals(tmp, additional_data);

        m.clear();
        m.put("additional_data", additional_data2);
        ch2.down(new Event(Event.CONFIG, m));
        local_addr=(IpAddress)ch2.getLocalAddress();
        assertNotNull(local_addr);
        tmp=local_addr.getAdditionalData();
        assertNotNull(tmp);
        assertEquals(tmp, additional_data2);
        assertFalse(Arrays.equals(tmp, additional_data));
    }

    @org.testng.annotations.Test
    public void testOrdering() throws Exception {
        final int NUM=100;
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        MyReceiver receiver=new MyReceiver(NUM);
        ch1.setReceiver(receiver);
        for(int i=1; i <= NUM; i++) {
            ch1.send(new Message(null, null, new Integer(i)));
            System.out.println("-- sent " + i);
        }

        receiver.waitForCompletion();

        List<Integer> nums=receiver.getNums();
        checkMonotonicallyIncreasingNumbers(nums);
        System.out.println(NUM + " messages were received in the correct order");
    }

    private static void checkMonotonicallyIncreasingNumbers(List<Integer> nums) {
        int current=-1;
        for(int num: nums) {
            if(current < 0) {
                current=num;
            }
            else {
                Assert.assertEquals(num, ++current, "list is " + nums);
            }
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<Integer> nums=new LinkedList<Integer>();
        final int expected;

        public MyReceiver(int expected) {
            this.expected=expected;
        }

        public List<Integer> getNums() {
            return nums;
        }

        public void waitForCompletion() throws InterruptedException {
            synchronized(nums) {
                while(nums.size() < expected) {
                    nums.wait();
                }
            }
        }

        public void receive(Message msg) {
            Util.sleepRandom(100);
            Integer num=(Integer)msg.getObject();
            synchronized(nums) {
                System.out.println("-- received " + num);
                nums.add(num);
                if(nums.size() >= expected) {
                    nums.notifyAll();
                }
            }
            Util.sleepRandom(100);
        }
    }

    
    public static Test suite() {
        return new TestSuite(MultiplexerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(MultiplexerTest.suite());
    }   

    private static class Cache extends ExtendedReceiverAdapter {
        protected final Map data ;
        Channel ch;
        String name;

        public Cache(Channel ch, String name) {
        	this.data=new TreeMap();
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
            synchronized(data) {
                data.put(key, val);
            }
            Message msg=new Message(null, null, buf);
            ch.send(msg);
        }

        protected int size() {
            synchronized(data) {
                return data.size();
            }
        }


        public void receive(Message msg) {
            if(ch.getLocalAddress().equals(msg.getSrc()))
                return;
            Object[] modification=(Object[])msg.getObject();
            Object key=modification[0];
            Object val=modification[1];
            synchronized(data) {
                // System.out.println("****** [" + name + "] received PUT(" + key + ", " + val + ") " + " from " + msg.getSrc() + " *******");
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

        public void getState(OutputStream ostream){
            ObjectOutputStream oos = null;
            try{
               oos = new ObjectOutputStream(ostream);
               synchronized(data){
                  oos.writeObject(data);
               }
               oos.flush();
            }
            catch (IOException e){}
            finally{
               try{
                  if(oos != null)
                     oos.close();
               }
               catch (IOException e){
                  System.err.println(e);
               }
            }
        }

        public void getState(String state_id, OutputStream ostream) {
           getState(ostream);
        }

        public void setState(InputStream istream) {
           ObjectInputStream ois = null;
           try {
               ois = new ObjectInputStream(istream);
               Map m = (Map)ois.readObject();
               synchronized (data)
               {
                  data.clear();
                  data.putAll(m);
               }

           } catch (Exception e) {}
           finally{
               try {
                   if(ois != null)
                      ois.close();
               } catch (IOException e) {
                   System.err.println(e);
               }
           }
        }

        public void setState(String state_id, InputStream istream) {
           setState(istream);
        }

        public void clear() {
            synchronized (data){
               data.clear();  
            }            
        }


        public void viewAccepted(View new_view) {
            log("view is " + new_view);
        }

        public String toString() {        	
        	synchronized(data){
        		return data.toString();
        	}            
        }


        public String printKeys() {
            return data.keySet().toString();
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
            Map copy=null;
            synchronized (data){
               copy=new HashMap(data); 
            }
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

        public void getState(String state_id,OutputStream os) {
           Map copy=null;
           synchronized (data){
              copy=new HashMap(data); 
           }           
           for(Iterator it=copy.keySet().iterator(); it.hasNext();) {
               Integer key=(Integer)it.next();
               if(state_id.equals("odd") && key.intValue() % 2 != 0)
                   it.remove();
               else if(state_id.equals("even") && key.intValue() % 2 == 0)
                   it.remove();
           }
           ObjectOutputStream oos = null;
           try {
               oos=new ObjectOutputStream(os);
               oos.writeObject(copy);
               oos.flush();
           }
           catch (IOException e){}
           finally{
              try{
                 if(oos != null)
                    oos.close();
              }
              catch (IOException e){
                 System.err.println(e);
              }
           }
        }

        public void setState(String state_id, InputStream is){
           setState(is);
        }

        public void setState(String state_id, byte[] state) {
            setState(state);
        }

        public String toString() {
            synchronized(data) {
                Set keys=new TreeSet(data.keySet());
                StringBuilder sb=new StringBuilder();
                for(Iterator it=keys.iterator(); it.hasNext();) {
                    Object o=it.next();
                    sb.append(o).append("=").append(data.get(o)).append(" ");
                }
                return sb.toString();
            }
        }
    }

}
