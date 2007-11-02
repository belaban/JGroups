package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.ChannelException;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.JChannel;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

/**
 * Tests transfer of large states (http://jira.jboss.com/jira/browse/JGRP-225).
 * Note that on Mac OS, FRAG2.frag_size and max_bundling_size in the transport should be less than 16'000 due to
 * http://jira.jboss.com/jira/browse/JGRP-560. As alternative, increase the MTU of the loopback device to a value
 * greater than max_bundle_size, e.g.
 * ifconfig lo0 mtu 65000
 * @author Bela Ban
 * @version $Id: LargeStateTransferTest.java,v 1.8 2007/11/02 13:04:26 vlada Exp $
 */
public class LargeStateTransferTest extends ChannelTestBase {
    JChannel provider, requester;
    Promise<Integer> p=new Promise<Integer>();    
    long start, stop;
    final static int SIZE_1=100000, SIZE_2=1000000, SIZE_3=5000000, SIZE_4=10000000;




    public LargeStateTransferTest(String name) {
        super(name);
    }
       
    public boolean useBlocking() {
        return true;
    }


    protected void setUp() throws Exception {
        super.setUp();       
        CHANNEL_CONFIG = System.getProperty("channel.conf.flush", "flush-udp.xml");
        provider=createChannel("A");
        requester=createChannel("A");
    }

    protected void tearDown() throws Exception {
        if(provider != null)
            provider.close();
        if(requester != null)
            requester.close();
        super.tearDown();
    }


    public void testStateTransfer1() throws ChannelException {
        _testStateTransfer(SIZE_1);
    }

    public void testStateTransfer2() throws ChannelException {
        _testStateTransfer(SIZE_2);
    }

    public void testStateTransfer3() throws ChannelException {
        _testStateTransfer(SIZE_3);
    }

    public void testStateTransfer4() throws ChannelException {
        _testStateTransfer(SIZE_4);
    }



    public void _testStateTransfer(int size) throws ChannelException {
        provider.setReceiver(new Provider(size));
        provider.connect("X");
        p.reset();
        requester.setReceiver(new Requester(p));
        requester.connect("X");
        log("requesting state of " + size + " bytes");
        start=System.currentTimeMillis();
        requester.getState(null, 20000);
        Object result=p.getResult(10000);
        stop=System.currentTimeMillis();
        log("result=" + result + " bytes (in " + (stop-start) + "ms)");
        assertNotNull(result);
        assertEquals(result, new Integer(size));
    }



    static void log(String msg) {
        System.out.println(Thread.currentThread() + " -- "+ msg);
    }

    public static Test suite() {
        return new TestSuite(LargeStateTransferTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(LargeStateTransferTest.suite());
    }


    private static class Provider extends ExtendedReceiverAdapter {
        byte[] state;

        public Provider(int size) {
            state=new byte[size];
        }

        public byte[] getState() {
            return state;
        }
        
        public void getState(OutputStream ostream){      
            ObjectOutputStream oos =null;
            try{
               oos=new ObjectOutputStream(ostream);
               oos.writeInt(state.length); 
               oos.write(state);
            }
            catch (IOException e){}
            finally{
               Util.close(ostream);
            }
        }
        public void setState(byte[] state) {
            throw new UnsupportedOperationException("not implemented by provider");
        }
    }


    private static class Requester extends ExtendedReceiverAdapter {
        Promise<Integer> p;

        public Requester(Promise<Integer> p) {
            this.p=p;
        }

        public byte[] getState() {
            throw new UnsupportedOperationException("not implemented by requester");
        }

        public void setState(byte[] state) {
            p.setResult(new Integer(state.length));
        }
        public void setState(InputStream istream) {
            ObjectInputStream ois=null;
            int size=0;
            try{
               ois= new ObjectInputStream(istream);
               size = ois.readInt();
               byte []stateReceived= new byte[size];
               ois.read(stateReceived);
            }
            catch (IOException e){                        }
            finally{
               Util.close(ois);
            }
            p.setResult(new Integer(size));
        }
    }

}
