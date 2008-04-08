package org.jgroups.blocks;

import java.util.Vector;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherAnycastTest.java,v 1.4 2008/04/08 07:19:09 belaban Exp $
 */
public class RpcDispatcherAnycastTest extends ChannelTestBase {
    RpcDispatcher disp, disp2, disp3;
    Channel ch, ch2, ch3;  

    public void setUp() throws Exception {
        ;

        ch=createChannel("A");
        ServerObject obj=new ServerObject(null);
        disp=new RpcDispatcher(ch, null, null, obj);
        ch.connect("demo");
        obj.setAddress(ch.getLocalAddress());

        ch2=createChannel("A");
        ServerObject obj2=new ServerObject(null);
        disp2=new RpcDispatcher(ch2, null, null, obj2);
        ch2.connect("demo");
        obj2.setAddress(ch2.getLocalAddress());

        ch3=createChannel("A");
        ServerObject obj3=new ServerObject(null);
        disp3=new RpcDispatcher(ch3, null, null, obj3);
        ch3.connect("demo");
        obj3.setAddress(ch3.getLocalAddress());
    }

    public void tearDown() throws Exception {
        ;


        ch3.close();
        disp3.stop();
        ch2.close();
        disp2.stop();
        ch.close();
        disp.stop();
    }



    public void testUnserializableValue() {
        Vector members=ch.getView().getMembers();
        System.out.println("members: " + members);
        assertTrue("we should have more than 1 member", members.size() > 1);

        Vector subset=Util.pickSubset(members, 0.2);
        System.out.println("subset: " + subset);

        Util.sleep(1000);

        RspList rsps=disp.callRemoteMethods(subset, "foo", null, (Class[])null, GroupRequest.GET_ALL, 0, false);
        System.out.println("rsps (no anycast): " + rsps);

        rsps=disp.callRemoteMethods(subset, "foo", null, (Class[])null, GroupRequest.GET_ALL, 0, true);
        System.out.println("rsps (with anycast): " + rsps);
    }


    static class ServerObject {
        Address addr;

        public ServerObject(Address addr) {
            this.addr=addr;
        }

        public Address foo() {
            System.out.println("foo() - returning " + addr);
            return addr;
        }

        public void setAddress(Address localAddress) {
            addr=localAddress;
        }
    }


    public static Test suite() {
        return new TestSuite(RpcDispatcherAnycastTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherAnycastTest.suite());
    }
}
