package org.jgroups.blocks;


import org.testng.annotations.*;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.FRAG;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.Map;
import java.util.Properties;
import java.util.Vector;

/**
 * A collection of tests to test the RpcDispatcher.
 * 
 * NOTE on processing return values: 
 * 
 * The method RspDispatcher.callRemoteMethods(...) returns an RspList, containing one Rsp
 * object for each group member receiving the RPC call. Rsp.getValue() returns the 
 * value returned by the RPC call from the corresponding member. Rsp.getValue() may
 * contain several classes of values, depending on what happened during the call:
 * 
 * (i) a value of the expected return data type, if the RPC call completed successfully 
 * (ii) null, if the RPC call timed out before the value could be returned
 * (iii) an object of type java.lang.Throwable, if an exception (e.g. lava.lang.OutOfMemoryException) 
 * was raised during the processing of the call 
 * 
 * It is wise to check for such cases when processing RpcDispatcher calls.
 * 
 * This also applies to the return value of callRemoteMethod(...).
 * 
 * @author Bela Ban
 * @version $Id: RpcDispatcherTest.java,v 1.16 2008/05/23 10:46:03 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class RpcDispatcherTest extends ChannelTestBase {
    RpcDispatcher disp1, disp2, disp3;
    JChannel c1, c2, c3;

    // specify return values sizes which should work correctly with 64Mb heap
    final static int[] SIZES={10000, 20000, 40000, 80000, 100000, 200000, 400000, 800000,
        1000000, 2000000, 5000000};
    // specify return value sizes which may generate timeouts or OOMEs with 64Mb heap
    final static int[] HUGESIZES={10000000, 20000000};

    @BeforeClass
    protected void setUp() throws Exception {
        c1=createChannel(true);
        final String props=c1.getProperties();
        final String GROUP=getUniqueClusterName("RpcDispatcherTest");
        disp1=new RpcDispatcher(c1, null, null, new ServerObject(1));
        c1.connect(GROUP);

        c2=createChannelWithProps(props);
        disp2=new RpcDispatcher(c2, null, null, new ServerObject(2));
        c2.connect(GROUP);

        c3=createChannelWithProps(props);
        disp3=new RpcDispatcher(c3, null, null, new ServerObject(3));
        c3.connect(GROUP);
    }

    @AfterClass
    protected void tearDown() throws Exception {
        disp3.stop();
        c3.close();
        disp2.stop();
        c2.close();
        disp1.stop();
        c1.close();
    }




    /**
     * Test the response filter mechanism which can be used to filter responses received with
     * a call to RpcDispatcher.
     * 
     * The test filters requests based on the id of the server object they were received
     * from, and only accept responses from servers with id > 1. 
     * 
     * The expected behaviour is that the response from server 1 is rejected, but the responses 
     * from servers 2 and 3 are accepted.
     *
     */
    @Test(groups="first")
    public void testResponseFilter() {
    	
    	final long timeout = 10 * 1000 ;
    	
        RspList rsps=disp1.callRemoteMethods(null, "foo", null, null,GroupRequest.GET_ALL, timeout, false,
                                             new RspFilter() {
                                                 int num=0;
                                                 public boolean isAcceptable(Object response, Address sender) {
                                                     boolean retval=((Integer)response).intValue() > 1;
                                                     // System.out.println("-- received " + response + " from " +
                                                     // sender + ": " + (retval ? "OK" : "NOTOK"));
                                                     if(retval)
                                                         num++;
                                                     return retval;
                                                 }

                                                 public boolean needMoreResponses() {
                                                     return num < 2;
                                                 }
                                             });
        System.out.println("responses are:\n" + rsps);
        assertEquals("there should be three response values", 3, rsps.size());
        assertEquals("number of responses received should be 2", 2, rsps.numReceived());
    }


    /**
     * Test the ability of RpcDispatcher to handle large argument and return values
     * with multicast RPC calls.
     * 
     * The test sends requests for return values (byte arrays) having increasing sizes,
     * which increase the processing time for requests as well as the amount of memory
     * required to process requests.
     * 
     * The expected behaviour is that all RPC requests complete successfully.
     *
     */
    @Test(groups="first")
    public void testLargeReturnValue() {
        setProperties();
        for(int i=0; i < SIZES.length; i++) {
            _testLargeValue(SIZES[i]);
        }
    }
    
    /**
     * Test the ability of RpcDispatcher to handle huge argument and return values
     * with multicast RPC calls.
     * 
     * The test sends requests for return values (byte arrays) having increasing sizes,
     * which increase the processing time for requests as well as the amount of memory
     * required to process requests.
     * 
     * The expected behaviour is that RPC requests either timeout or trigger out of 
     * memory exceptions. Huge return values extend the processing time required; but
     * the length of time depends upon the speed of the machine the test runs on. 
     *
     */
    @Test(groups="first")
    public void testHugeReturnValue() {
        setProperties();
        for(int i=0; i < HUGESIZES.length; i++) {
            _testHugeValue(HUGESIZES[i]);
        }
    }
    

    /**
     * Tests a method call to {A,B,C} where C left *before* the call. http://jira.jboss.com/jira/browse/JGRP-620
     */
    @Test(dependsOnGroups="first")
    public void testMethodInvocationToNonExistingMembers() {
    	
    	final int timeout = 5 * 1000 ;
    	
    	// get the current membership, as seen by C
        View view=c3.getView();
        Vector<Address> members=view.getMembers();
        System.out.println("list is " + members);

        // cause C to leave the group and close its channel
        System.out.println("closing c3");
        c3.close();

        Util.sleep(1000);
        
        // make an RPC call using C's now outdated view of membership
        System.out.println("calling method foo() in " + members + " (view=" + c2.getView() + ")");
        RspList rsps=disp1.callRemoteMethods(members, "foo", null, (Class[])null, GroupRequest.GET_ALL, timeout);
        
        // all responses 
        System.out.println("responses:\n" + rsps);
        for(Map.Entry<Address,Rsp> entry: rsps.entrySet()) {
            Rsp rsp=entry.getValue();
            assertTrue("response from " + entry.getKey() + " was not received", rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }
    }


    /**
     * Test the ability of RpcDispatcher to handle large argument and return values
     * with unicast RPC calls.
     * 
     * The test sends requests for return values (byte arrays) having increasing sizes,
     * which increase the processing time for requests as well as the amount of memory
     * required to process requests.
     * 
     * The expected behaviour is that all RPC requests complete successfully.
     *
     */
    @Test(groups="first")
    public void testLargeReturnValueUnicastCall() throws Throwable {
        setProperties();
        for(int i=0; i < SIZES.length; i++) {
            _testLargeValueUnicastCall(c1.getLocalAddress(), SIZES[i]);
        }
    }

    private void setProperties() {
        setProps(c1); setProps(c2); setProps(c3);
    }


    private static void setProps(JChannel ch) {
        Protocol prot=ch.getProtocolStack().findProtocol("FRAG2");
        if(prot != null) {
            ((FRAG2)prot).setFragSize(12000);
        }
        prot=ch.getProtocolStack().findProtocol("FRAG");
        if(prot != null) {
            ((FRAG)prot).setFragSize(12000);
        }

        prot=ch.getProtocolStack().getTransport();
        if(prot != null)
            ((TP)prot).setMaxBundleSize(14000);
    }

    /**
     * Helper method to perform a RPC call on server method "returnValue(int size)" for 
     * all group members.
     * 
     * The method checks that each returned value is non-null and has the correct size. 
     *    
     */
    void _testLargeValue(int size) {
    	
    	// 20 second timeout 
    	final long timeout = 20 * 1000 ;
    		
        System.out.println("\ntesting with " + size + " bytes");
        RspList rsps=disp1.callRemoteMethods(null, "largeReturnValue", new Object[]{size}, new Class[]{int.class}, GroupRequest.GET_ALL, timeout);
        System.out.println("rsps:");
        assert rsps.size() == 3 : "there should be three responses to the RPC call but only " + rsps.size() +
                " were received: " + rsps;
        
        for(Map.Entry<Address,Rsp> entry: rsps.entrySet()) {
        	
        	// its possible that an exception was raised in processing
        	Object obj = entry.getValue().getValue() ;
        	
        	// this should not happen
        	assert !(obj instanceof Throwable) : "exception was raised in processing reasonably sized argument";
        	
            byte[] val=(byte[]) obj;
            assert val != null;
            System.out.println(val.length + " bytes from " + entry.getValue().getSender());
            assert val.length == size : "return value does not match required size";
        }
    }
    
    /**
     * Helper method to perform a RPC call on server method "returnValue(int size)" for 
     * all group members.
     * 
     * This method need to take into account that RPC calls can timeout with huge values,
     * and they can also trigger OOMEs. But if we are lucky, they can also return
     * reasonable values. 
     * 
     */
    void _testHugeValue(int size) {
    	
    	// 20 second timeout 
    	final long timeout = 20 * 1000 ;
    	
        System.out.println("\ntesting with " + size + " bytes");
        RspList rsps=disp1.callRemoteMethods(null, "largeReturnValue", new Object[]{size}, new Class[]{int.class}, GroupRequest.GET_ALL, timeout);
        System.out.println("rsps:");
        assert rsps != null;
        assert rsps.size() == 3 : "there should be three responses to the RPC call but only " + rsps.size() +
                " were received: " + rsps;

        // in checking the return values, we need to take account of timeouts (i.e. when
        // a null value is returned) and exceptions 
        for(Map.Entry<Address,Rsp> entry: rsps.entrySet()) {

        	Object obj = entry.getValue().getValue() ;

        	// its possible that an exception was raised
        	if (obj instanceof java.lang.Throwable) {
        		Throwable t = (Throwable) obj ;
        		
        		System.out.println(t.toString() + " exception was raised processing argument from " +
        							entry.getValue().getSender() + " -this is expected") ;
        		continue ;
        	}        	
        	
        	// its possible that the request timed out before the serve could reply 
        	if (obj == null) {
        		System.out.println("request timed out processing argument from " + 
        							entry.getValue().getSender() + " - this is expected") ;
        		continue ;       	
        	}
        	
        	// if we reach here, we sould have a reasobable value
        	byte[] val=(byte[]) obj;
            System.out.println(val.length + " bytes from " + entry.getValue().getSender());
            assert val.length == size : "return value does not match required size";
        }
    }

    /**
     * Helper method to perform a RPC call on server method "returnValue(int size)" for 
     * an individual group member. 
     * 
     * The method checks that the returned value is non-null and has the correct size. 
     * 
     * @param dst the group member
     * @param size the size of the byte array to be returned
     * @throws Throwable
     */
    void _testLargeValueUnicastCall(Address dst, int size) throws Throwable {
    	
    	// 20 second timeout
    	final long timeout = 20 * 1000 ;
    	
        System.out.println("\ntesting unicast call with " + size + " bytes");
        assertNotNull(dst);
        
        Object retval=disp1.callRemoteMethod(dst, "largeReturnValue", new Object[]{size}, new Class[]{int.class}, GroupRequest.GET_ALL, timeout);

    	// it's possible that an exception was raised
        if (retval instanceof java.lang.Throwable) {
        	throw (Throwable)retval;
        }
        
        byte[] val=(byte[])retval;
        
        // check value is not null, otherwise fail the test
        assertNotNull("return value should be non-null", val);
        System.out.println("rsp: " + val.length + " bytes");
        
        // returned value should have requested size
        assertEquals("return value does not match requested size", size, val.length);
    }

    /**
     * This class serves as a server obect to turn requests into replies.
     * It is initialised with an integer id value.
     * 
     * It implements two functions:
     * function foo() returns the id of the server
     * function largeReturnValue(int size) returns a byte array of size 'size'
     *  
     */
    private static class ServerObject {
        int i;
        public ServerObject(int i) {
            this.i=i;
        }
        public int foo() {return i;}


        public static byte[] largeReturnValue(int size) {
            return new byte[size];
        }
    }


}