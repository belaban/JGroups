package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.*;
import org.jgroups.protocols.TCPPING;
import org.jgroups.mux.MuxChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;

import java.io.*;
import java.util.*;

/**
 * Test the multiplexer functionality provided by JChannelFactory
 * @author Bela Ban
 * @version $Id: MultiplexerStateTransferTest.java,v 1.11 2008/05/23 10:45:42 belaban Exp $
 */
@Test(groups=Global.MULTIPLEXER)
public class MultiplexerStateTransferTest extends ChannelTestBase {
    private Cache c1, c2, c1_repl, c2_repl;
    private Channel ch1, ch2, ch1_repl, ch2_repl;
    JChannelFactory factory, factory2;   


    @BeforeMethod
    public void setUp() throws Exception {
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
            assert !((MuxChannel)ch1).getChannel().isOpen();
            assert !((MuxChannel)ch1).getChannel().isConnected();
        }
        if(ch2 != null) {
            assert !((MuxChannel)ch2).getChannel().isOpen();
            assert !((MuxChannel)ch2).getChannel().isConnected();
        }
        if(ch1_repl != null) {
            assert !((MuxChannel)ch1_repl).getChannel().isOpen();
            assert !((MuxChannel)ch1_repl).getChannel().isConnected();
        }
        if(ch2_repl != null) {
            assert !((MuxChannel)ch2_repl).getChannel().isOpen();
            assert !((MuxChannel)ch2_repl).getChannel().isConnected();
        }

        if(c1 != null) c1.clear();
        if(c2 != null) c2.clear();
        if(c1_repl != null) c1_repl.clear();
        if(c2_repl != null) c2_repl.clear();

        ch1_repl=ch2_repl=ch1=ch2=null;
        c1=c2=c1_repl=c2_repl=null; 
    }
    
    @Test
    public void testStateTransfer() throws Exception {
        regularStateTransfer(false); 
    }
    
    @Test
    public void testConnectAndStateTransfer() throws Exception {
        regularStateTransfer(true); 
    }
    
    private void regularStateTransfer(boolean joinAndFetch) throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        c1=new Cache(ch1, "cache-1");
        if(joinAndFetch){
            ch1.connect("bla", null, null, 5000);
        }
        else{
            ch1.connect("bla");
        }
        assert c1.size() == 0 : "cache has to be empty initially";

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");

        c1.put("name", "Bela");
        c1.put("id", new Long(322649));
        c1.put("hobbies", "biking");
        c1.put("bike", "Centurion");


        if(joinAndFetch){
            ch1_repl.connect("bla", null, null,5000);
        }
        else{
            ch1_repl.connect("bla");           
            boolean rc=ch1_repl.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }
        Util.sleep(500);

        System.out.println("c1_repl: " + c1_repl);
        assert c1_repl.size() == 4 : "initial state should have been transferred";

        assert c1.get("id").equals(322649);
        assert c1_repl.get("id").equals(322649);

        assert c1.get("hobbies").equals("biking");
        assert c1_repl.get("hobbies").equals("biking");

        assert c1.get("bike").equals("Centurion");
        assert c1_repl.get("bike").equals("Centurion");
    }

    @Test
    public void testStateTransferWithTwoApplications() throws Exception {
        stateTransferWithTwoApplications(false);
    }
    
    @Test
    public void testConnectAndStateTransferWithTwoApplications() throws Exception {
        stateTransferWithTwoApplications(true);
    }

    private void stateTransferWithTwoApplications(boolean joinAndFetch) throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        c1=new Cache(ch1, "cache-1");
        
        if(joinAndFetch){
            ch1.connect("bla", null, null, 5000);
        }else{
            ch1.connect("bla");
        }
        
        assert c1.size() == 0 : "cache has to be empty initially";

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        c2=new Cache(ch2, "cache-2");
        if(joinAndFetch) {
            ch2.connect("bla", null, null, 5000);
        } else {
            ch2.connect("bla");
        }
        assert c2.size() == 0 : "cache has to be empty initially";

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");

        ch2_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c2");


        c1.put("name", "cache-1");
        c2.put("name", "cache-2");

        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        if(joinAndFetch) {
            ch1_repl.connect("bla", null, null, 5000);
        } else {
            ch1_repl.connect("bla");
            boolean rc=ch1_repl.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }        

        c2_repl=new Cache(ch2_repl, "cache-2-repl");
        
        if(joinAndFetch){
            ch2_repl.connect("bla", null, null, 5000);
        }else{
            ch2_repl.connect("bla");
            boolean rc=ch2_repl.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c1_repl: " + c1_repl);
        System.out.println("c2: " + c2);
        System.out.println("c2_repl: " + c2_repl);

        assert c1.size() == 1;
        assert c1_repl.size() == 1;

        assert c2.size() == 1;
        assert c2_repl.size() == 1;

        assert c1.get("name").equals("cache-1");
        assert c1_repl.get("name").equals("cache-1");

        assert c2.get("name").equals("cache-2");
        assert c2_repl.get("name").equals("cache-2");
    }


    @Test
    public void testStateTransferWithRegistration() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1",true,null);
        ch1.connect("bla");
        c1=new Cache(ch1, "cache-1");
        boolean rc = ch1.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        assert c1.size() == 0 : "cache has to be empty initially";

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2",true,null);
        ch2.connect("bla");
        c2=new Cache(ch2, "cache-2");
        rc = ch2.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        assert c2.size() == 0 : "cache has to be empty initially";
        
        c1.put("name", "cache-1");
        c2.put("name", "cache-2");

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1", true, null); // register for state transfer
        ch2_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c2", true, null); // register for state transfer

        ch1_repl.connect("bla");
        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        rc=ch1_repl.getState(null, 5000); // this will *not* trigger the state transfer protocol
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

        assert c1.size() == 1;
        assert c1_repl.size() == 1;

        assert c2.size() == 1;
        assert c2_repl.size() == 1;

        assert c1.get("name").equals("cache-1");
        assert c1_repl.get("name").equals("cache-1");

        assert c2.get("name").equals("cache-2");
        assert c2_repl.get("name").equals("cache-2");
        c1.clear();
        c1_repl.clear();
        c2.clear();
        c2_repl.clear();
    }


    private static void setCorrectPortRange(Channel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol tcpping=stack.findProtocol("TCPPING");
        if(tcpping == null)
            return;

        TCPPING ping=(TCPPING)tcpping;
        int port_range=ping.getPortRange();
        System.out.println("port_range in TCPPING: " + port_range + ", setting it to 2");
        ping.setPortRange(2);
    }


    @Test
    public void testStateTransferWithReconnect() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        setCorrectPortRange(ch1);

        assertTrue(ch1.isOpen());
        assert !ch1.isConnected();
        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());
        assertServiceAndClusterView(ch1, 1, 1);

        c1=new Cache(ch1, "cache-1");
        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");
        setCorrectPortRange(ch1_repl);
        assertTrue(ch1_repl.isOpen());
        assert !ch1_repl.isConnected();

        c1.put("name", "Bela");
        c1.put("id", new Long(322649));
        c1.put("hobbies", "biking");
        c1.put("bike", "Centurion");

        ch1_repl.connect("bla");
        assertTrue(ch1_repl.isOpen());
        assertTrue(ch1_repl.isConnected());
        assertServiceAndClusterView(ch1_repl, 2, 2);
        Util.sleep(500);
        assertServiceAndClusterView(ch1, 2, 2);

        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        boolean rc=ch1_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("c1_repl: " + c1_repl);
        Assert.assertEquals(c1_repl.size(), 4, "initial state should have been transferred");
        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));

        ch1_repl.disconnect();
        assertTrue(ch1_repl.isOpen());
        assert !ch1_repl.isConnected();
        Util.sleep(1000);
        assertServiceAndClusterView(ch1, 1, 1);

        c1_repl.clear();

        ch1_repl.connect("bla");
        assertTrue(ch1_repl.isOpen());
        assertTrue(ch1_repl.isConnected());
        assertServiceAndClusterView(ch1_repl, 2, 2);
        Util.sleep(300);
        assertServiceAndClusterView(ch1, 2, 2);

        Assert.assertEquals(c1_repl.size(), 0, "cache has to be empty initially");

        rc=ch1_repl.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("c1_repl: " + c1_repl);
        Assert.assertEquals(c1_repl.size(), 4, "initial state should have been transferred");

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));

        // Now see what happens if we reconnect the first channel
        // But first, add another MuxChannel on that JChannel
        // just so it remains coordinator (test that it doesn't
        // ask for state from itself)
        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        setCorrectPortRange(ch2);
        assertTrue(ch2.isOpen());
        assert !ch2.isConnected();
        assertServiceAndClusterView(ch1, 2, 2);
        assertServiceAndClusterView(ch1_repl, 2, 2);


        ch1.disconnect();
        //sleep a bit and thus let asynch VIEW to propagate to other channel
        Util.sleep(500);
        assertTrue(ch1.isOpen());
        assert !ch1.isConnected();
        assertServiceAndClusterView(ch1_repl, 1, 1);
        assertTrue(ch2.isOpen());
        assert !ch2.isConnected();

        c1.clear();

        ch1.connect("bla");
        assertTrue(ch1.isOpen());
        assertTrue(ch1.isConnected());
        assertServiceAndClusterView(ch1, 2, 2);
        Util.sleep(500);
        assertServiceAndClusterView(ch1_repl, 2, 2);
        assertTrue(ch2.isOpen());
        assert !ch2.isConnected();

        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");

        rc=ch1.getState(null, 5000);
        System.out.println("state transfer: " + rc);
        Util.sleep(500);

        System.out.println("c1: " + c1);
        Assert.assertEquals(c1.size(), 4, "initial state should have been transferred");

        assertEquals(new Long(322649), c1.get("id"));
        assertEquals(new Long(322649), c1_repl.get("id"));

        assertEquals("biking", c1.get("hobbies"));
        assertEquals("biking", c1_repl.get("hobbies"));

        assertEquals("Centurion", c1.get("bike"));
        assertEquals("Centurion", c1_repl.get("bike"));
    }


    private static void assertServiceAndClusterView(Channel ch, int num_service_view_mbrs, int num_cluster_view_mbrs) {
        View service_view, cluster_view;
        service_view=ch.getView();
        cluster_view=((MuxChannel)ch).getClusterView();

        String msg="cluster view=" + cluster_view + ", service view=" + service_view;

        assertNotNull(service_view);
        assertNotNull(cluster_view);

        Assert.assertEquals(service_view.size(), num_service_view_mbrs, msg);
        Assert.assertEquals(cluster_view.size(), num_cluster_view_mbrs, msg);
    }   

    @Test
    public void testStateTransferFromSelf() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        boolean rc=ch1.getState(null, 2000);
        assert !rc : "getState() on singleton should return false";
        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        ch2.connect("foo");
        rc=ch2.getState(null, 2000);
        assert !rc : "getState() on singleton should return false";
    }


    @Test
    public void testGetSubstates() throws Exception {
        getSubstates(false);
    }
   
    @Test
    public void testConnectAndGetSubstates() throws Exception {
        getSubstates(true);
    }
    
    private void getSubstates(boolean joinAndFetch) throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        c1=new ExtendedCache(ch1, "cache-1");
        if(joinAndFetch){
            ch1.connect("bla", null, null, 5000);
        }else{
            ch1.connect("bla");
        }

        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        c2=new ExtendedCache(ch2, "cache-2");
        
        if(joinAndFetch){
            ch2.connect("bla", null, null, 5000);
        }else{
            ch2.connect("bla");
        }

        Assert.assertEquals(c2.size(), 0, "cache has to be empty initially");

        for(int i=0; i < 10; i++) {
            c1.put(new Integer(i), new Integer(i));
            c2.put(new Integer(i), new Integer(i));
        }

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");
        ch2_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c2");
        c1_repl=new ExtendedCache(ch1_repl, "cache-1-repl");
        
        if (joinAndFetch) {
            ch1_repl.connect("bla", null, "odd", 5000);
        } else {
            ch1_repl.connect("bla");
            boolean rc = ch1_repl.getState(null, "odd", 5000);
            System.out.println("state transfer: " + rc);
        }        

        c2_repl=new ExtendedCache(ch2_repl, "cache-2-repl");
        if (joinAndFetch) {
            ch2_repl.connect("bla", null, "even", 5000);
        } else {
            ch2_repl.connect("bla");
            boolean rc = ch2_repl.getState(null, "even", 5000);
            System.out.println("state transfer: " + rc);
        }               
        Util.sleep(500);

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c2: " + c2);

        System.out.println("c1_repl (removed odd substate): " + c1_repl);
        System.out.println("c2_repl (removed even substate): " + c2_repl);

        Assert.assertEquals(5, c1_repl.size());
        Assert.assertEquals(5, c2_repl.size());

        _testEvenNumbersPresent(c1_repl);
        _testOddNumbersPresent(c2_repl);
    }

    private static void _testEvenNumbersPresent(Cache c) {
        Integer[] evens=new Integer[]{new Integer(0), new Integer(2), new Integer(4), new Integer(6), new Integer(8)};
        _testNumbersPresent(c, evens);

    }

    private static void _testOddNumbersPresent(Cache c) {
        Integer[] odds=new Integer[]{new Integer(1), new Integer(3), new Integer(5), new Integer(7), new Integer(9)};
        _testNumbersPresent(c, odds);
    }

    private static void _testNumbersPresent(Cache c, Integer[] numbers) {
        int len=numbers.length;
        Assert.assertEquals(len, c.size());
        for(int i=0; i < numbers.length; i++) {
            Integer number=numbers[i];
            assertEquals(number, c.get(number));
        }
    }



    @Test
    public void testGetSubstatesMultipleTimes() throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        ch1.connect("bla");
        c1=new ExtendedCache(ch1, "cache-1");
        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");

        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        ch2.connect("bla");
        c2=new ExtendedCache(ch2, "cache-2");
        Assert.assertEquals(c2.size(), 0, "cache has to be empty initially");

        for(int i=0; i < 10; i++) {
            c1.put(new Integer(i), new Integer(i));
            c2.put(new Integer(i), new Integer(i));
        }

        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");
        ch2_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c2");
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

        Assert.assertEquals(5, c2_repl.size());
        rc=ch2_repl.getState(null, "odd", 5000);
        Util.sleep(500);
        System.out.println("c2_repl (removed odd substate): " + c2_repl);
        _testEvenNumbersPresent(c2_repl);

        Assert.assertEquals(5, c2_repl.size());
        rc=ch2_repl.getState(null, "even", 5000);
        Util.sleep(500);
        System.out.println("c2_repl (removed even substate): " + c2_repl);
        _testOddNumbersPresent(c2_repl);

        Assert.assertEquals(5, c2_repl.size());
        rc=ch2_repl.getState(null, "odd", 5000);
        Util.sleep(500);
        System.out.println("c2_repl (removed odd substate): " + c2_repl);
        _testEvenNumbersPresent(c2_repl);
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
