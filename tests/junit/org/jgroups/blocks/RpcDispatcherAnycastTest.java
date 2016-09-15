package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT)
public class RpcDispatcherAnycastTest extends ChannelTestBase {
    RpcDispatcher disp, disp2, disp3;
    JChannel ch, ch2, ch3;

    @BeforeMethod
    void setUp() throws Exception {
        ch=createChannel(true,3);
        ServerObject obj=new ServerObject(null);
        disp=new RpcDispatcher(ch, obj);
        ch.connect("RpcDispatcherAnycastTest");
        obj.setAddress(ch.getAddress());

        ch2=createChannel(ch);
        ServerObject obj2=new ServerObject(null);
        disp2=new RpcDispatcher(ch2, obj2);
        ch2.connect("RpcDispatcherAnycastTest");
        obj2.setAddress(ch2.getAddress());

        ch3=createChannel(ch);
        ServerObject obj3=new ServerObject(null);
        disp3=new RpcDispatcher(ch3, obj3);
        ch3.connect("RpcDispatcherAnycastTest");
        obj3.setAddress(ch3.getAddress());
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, ch, ch2, ch3);
    }

    @AfterMethod
    void tearDown() throws Exception {
        ch3.close();
        disp3.stop();
        ch2.close();
        disp2.stop();
        ch.close();
        disp.stop();
    }



    public void testUnserializableValue() throws Exception {
        List<Address> members=ch.getView().getMembers();
        System.out.println("members: " + members);
        assert members.size() > 1: "we should have more than 1 member";

        List<Address> subset=Util.pickSubset(members, 0.2);
        System.out.println("subset: " + subset);

        Util.sleep(1000);

        RspList rsps=disp.callRemoteMethods(subset, "foo", null, null, new RequestOptions(ResponseMode.GET_ALL, 0, false));
        System.out.println("rsps (no anycast): " + rsps);

        rsps=disp.callRemoteMethods(subset, "foo", null, null, new RequestOptions(ResponseMode.GET_ALL, 0, true));
        System.out.println("rsps (with anycast): " + rsps);
    }


    static class ServerObject {
        Address addr;

        public ServerObject(Address addr) {
            this.addr=addr;
        }

        public Address foo() {
            // System.out.println("foo() - returning " + addr);
            return addr;
        }

        public void setAddress(Address localAddress) {
            addr=localAddress;
        }
    }


 
}
