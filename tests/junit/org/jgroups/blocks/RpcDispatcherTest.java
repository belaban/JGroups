package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherTest.java,v 1.8.2.2 2007/11/20 11:22:36 belaban Exp $
 */
public class RpcDispatcherTest extends ChannelTestBase {
    RpcDispatcher disp1, disp2, disp3;
    JChannel c1, c2, c3;

    final static int[] SIZES={10000, 20000, 40000, 80000, 100000, 200000, 400000, 800000,
            1000000, 2000000, 5000000, 10000000, 20000000};

    protected void setUp() throws Exception {
        super.setUp();
        c1=(JChannel)createChannel("A");
        disp1=new RpcDispatcher(c1, null, null, new ServerObject(1));
        c1.connect("demo");
        c2=(JChannel)createChannel("A");
        disp2=new RpcDispatcher(c2, null, null, new ServerObject(2));
        c2.connect("demo");
        c3=(JChannel)createChannel("A");
        disp3=new RpcDispatcher(c3, null, null, new ServerObject(3));
        c3.connect("demo");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        disp3.stop();
        c3.close();
        disp2.stop();
        c2.close();
        disp1.stop();
        c1.close();
    }

    public void foo() {

    }





    /**
     * Tests a method call to {A,B,C} where C left *before* the call. http://jira.jboss.com/jira/browse/JGRP-620
     */
    public void testMethodInvocationToNonExistingMembers() {
        View view=c3.getView();
        Vector<Address> members=view.getMembers();
        System.out.println("list is " + members);

        System.out.println("closing c3");
        c3.close();

        Util.sleep(1000);
        System.out.println("calling method foo() in " + members + " (view=" + c2.getView() + ")");
        RspList rsps=disp1.callRemoteMethods(members, "foo", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("responses:\n" + rsps);
        Map.Entry entry;
        for(Iterator<Map.Entry> it=rsps.entrySet().iterator(); it.hasNext();) {
            entry=it.next();
            Rsp rsp=(Rsp)entry.getValue();
            assertTrue("response from " + entry.getKey() + " was not received", rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }
    }



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


    public static Test suite() {
        return new TestSuite(RpcDispatcherTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherTest.suite());
    }
}