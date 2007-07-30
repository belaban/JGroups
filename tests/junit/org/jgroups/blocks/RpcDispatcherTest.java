package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.Channel;
import org.jgroups.Address;
import org.jgroups.util.RspList;
import org.jgroups.tests.ChannelTestBase;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherTest.java,v 1.3 2007/07/30 07:05:41 belaban Exp $
 */
public class RpcDispatcherTest extends ChannelTestBase {
    RpcDispatcher disp1, disp2, disp3;
    Channel c1, c2, c3;

    protected void setUp() throws Exception {
        super.setUp();
        c1=createChannel("A");
        disp1=new RpcDispatcher(c1, null, null, new ServerObject(1));
        c1.connect("demo");
        c2=createChannel("A");
        disp2=new RpcDispatcher(c2, null, null, new ServerObject(2));
        c2.connect("demo");
        c3=createChannel("A");
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


    public void testResponseFilter() {
        RspList rsps=disp1.callRemoteMethods(null, "foo", null, null,GroupRequest.GET_ALL, 10000, false,
                                             new RspFilter() {
                                                 int num=0;
                                                 public boolean isAcceptable(Object response, Address sender) {
                                                     boolean retval=((Integer)response).intValue() > 1;
                                                     System.out.println("-- received " + response + " from " +
                                                     sender + ": " + (retval ? "OK" : "NOTOK"));
                                                     if(retval)
                                                         num++;
                                                     return retval;
                                                 }

                                                 public boolean needMoreResponses() {
                                                     return num < 2;
                                                 }
                                             });
        System.out.println("responses are:\n" + rsps);
        assertEquals(3, rsps.size());
        assertEquals(2, rsps.numReceived());
    }


    private static class ServerObject {
        int i;
        public ServerObject(int i) {
            this.i=i;
        }
        public int foo() {return i;}
    }

    public static Test suite() {
        return new TestSuite(RpcDispatcherTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherTest.suite());
    }
}