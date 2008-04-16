package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.*;
import org.jgroups.mux.MuxChannel;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests various intermixed combinations of regular connect and get state with connect-and-get-state 
 * @author Vladimir Blagojevic
 * @version $Id: MultiplexerConnectStateTransferMixTest.java,v 1.6 2008/04/16 08:01:36 belaban Exp $
 */
@Test(groups=Global.MULTIPLEXER)
public class MultiplexerConnectStateTransferMixTest extends ChannelTestBase {
    private Cache c1, c2,c3, c1_repl, c2_repl,c3_repl;
    private Channel ch1, ch2, ch3, ch1_repl, ch2_repl,ch3_repl;
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
        if(ch3_repl != null)
            ch3_repl.close();
        if(ch1 != null)
            ch1.close();
        if(ch2 != null)
            ch2.close();       
        if(ch3 != null)
            ch3.close();
        
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
        if(c3 != null) c3.clear();
        if(c1_repl != null) c1_repl.clear();
        if(c2_repl != null) c2_repl.clear();
        if(c3_repl != null) c3_repl.clear();

        ch1_repl=ch2_repl=ch1=ch2=ch3=null;
        c1=c2=c1_repl=c2_repl=c3_repl=null; 
    }
    
    @Test
    public void testConnectStateTransferMixing() throws Exception{
        stateTransferWithIntermixedTransferTypes(new boolean[]{true,false,true}); 
    }
    
    @Test
    public void testConnectStateTransferMixing2() throws Exception{
        stateTransferWithIntermixedTransferTypes(new boolean[]{true,true,false}); 
    }
    
    @Test
    public void testConnectStateTransferMixing3() throws Exception{
        stateTransferWithIntermixedTransferTypes(new boolean[]{false,true,false}); 
    }
    
    @Test
    public void testConnectStateTransferMixing4() throws Exception{
        stateTransferWithIntermixedTransferTypes(new boolean[]{false,false,true}); 
    }
    
    private void stateTransferWithIntermixedTransferTypes(boolean [] connectTypes) throws Exception {
        ch1=factory.createMultiplexerChannel(mux_conf_stack, "c1");
        c1=new Cache(ch1, "cache-1");
        
        if(connectTypes[0]){
            System.out.println("Connect and state transfer");
            ch1.connect("bla", null, null, 5000);
        }else{
            System.out.println("Regular connect");
            ch1.connect("bla");
            boolean rc=ch1.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }

        Assert.assertEquals(c1.size(), 0, "cache has to be empty initially");

        
        ch2=factory.createMultiplexerChannel(mux_conf_stack, "c2");
        c2=new Cache(ch2, "cache-2");
        
        if(connectTypes[1]){
            System.out.println("Connect and state transfer");
            ch2.connect("bla", null, null, 5000);
        }else{
            System.out.println("Regular connect");
            ch2.connect("bla");
            boolean rc=ch2.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }

        Assert.assertEquals(c2.size(), 0, "cache has to be empty initially");
        
        ch3=factory.createMultiplexerChannel(mux_conf_stack, "c3");
        c3=new Cache(ch3, "cache-3");
        
        if(connectTypes[2]){
            System.out.println("Connect and state transfer");
            ch3.connect("bla", null, null, 5000);
        }else{
            System.out.println("Regular connect");
            ch3.connect("bla");
            boolean rc=ch3.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }

        Assert.assertEquals(c2.size(), 0, "cache has to be empty initially");


        ch1_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c1");

        ch2_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c2");

        ch3_repl=factory2.createMultiplexerChannel(mux_conf_stack, "c3");

        c1.put("name", "cache-1");
        c2.put("name", "cache-2");
        c3.put("name", "cache-3");

        c1_repl=new Cache(ch1_repl, "cache-1-repl");
        if(connectTypes[0]){
            System.out.println("Connect and state transfer");
            ch1_repl.connect("bla", null, null, 5000);
        }else{
            System.out.println("Regular connect");
            ch1_repl.connect("bla");
            boolean rc=ch1_repl.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }        

        c2_repl=new Cache(ch2_repl, "cache-2-repl");
        
        if(connectTypes[1]){
            System.out.println("Connect and state transfer");
            ch2_repl.connect("bla", null, null, 5000);
        }else{
            System.out.println("Regular connect");
            ch2_repl.connect("bla");
            boolean rc=ch2_repl.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }
        
        c3_repl=new Cache(ch3_repl, "cache-3-repl");
        
        if(connectTypes[2]){
            System.out.println("Connect and state transfer");
            ch3_repl.connect("bla", null, null, 5000);
        }else{
            System.out.println("Regular connect");
            ch3_repl.connect("bla");
            boolean rc=ch3_repl.getState(null, 5000);
            System.out.println("state transfer: " + rc);
        }

        System.out.println("Caches after state transfers:");
        System.out.println("c1: " + c1);
        System.out.println("c1_repl: " + c1_repl);
        System.out.println("c2: " + c2);
        System.out.println("c2_repl: " + c2_repl);
        System.out.println("c3: " + c3);
        System.out.println("c3_repl: " + c3_repl);

        Assert.assertEquals(1, c1.size());
        Assert.assertEquals(1, c1_repl.size());

        Assert.assertEquals(1, c2.size());
        Assert.assertEquals(1, c2_repl.size());

        Assert.assertEquals(1, c3.size());
        Assert.assertEquals(1, c3_repl.size());

        assertEquals("cache-1", c1.get("name"));
        assertEquals("cache-1", c1_repl.get("name"));

        assertEquals("cache-2", c2.get("name"));
        assertEquals("cache-2", c2_repl.get("name"));
        
        assertEquals("cache-3", c3.get("name"));
        assertEquals("cache-3", c3_repl.get("name"));
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
}
