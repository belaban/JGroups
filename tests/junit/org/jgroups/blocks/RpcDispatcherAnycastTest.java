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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT)
public class RpcDispatcherAnycastTest extends ChannelTestBase {
    protected RpcDispatcher disp, disp2, disp3;
    protected JChannel a, b, c;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel(true, 3).name("A");
        ServerObject obj=new ServerObject(null);
        disp=new RpcDispatcher(a, obj);
        a.connect("RpcDispatcherAnycastTest");
        obj.setAddress(a.getAddress());

        b=createChannel(a).name("B");
        ServerObject obj2=new ServerObject(null);
        disp2=new RpcDispatcher(b, obj2);
        b.connect("RpcDispatcherAnycastTest");
        obj2.setAddress(b.getAddress());

        c=createChannel(a).name("C");
        ServerObject obj3=new ServerObject(null);
        disp3=new RpcDispatcher(c, obj3);
        c.connect("RpcDispatcherAnycastTest");
        obj3.setAddress(c.getAddress());
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c);
    }

    @AfterMethod void tearDown() throws Exception {
        Util.close(disp3, disp2, disp, c,b,a);
    }



    public void testUnserializableValue() throws Exception {
        List<Address> members=a.getView().getMembers();
        System.out.println("members: " + members);
        assert members.size() > 1: "we should have more than 1 member";

        List<Address> subset=Collections.singletonList(b.getAddress());
        RspList<Address> rsps=disp.callRemoteMethods(subset, "foo", null, null, new RequestOptions(ResponseMode.GET_ALL, 0, false));
        System.out.println("rsps (no anycast): " + rsps);
        assert rsps.size() == 1;
        assert rsps.containsKey(b.getAddress());

        rsps=disp.callRemoteMethods(subset, "foo", null, null, new RequestOptions(ResponseMode.GET_ALL, 0, true));
        System.out.println("rsps (with anycast): " + rsps);
        assert rsps.size() == 1;
        assert rsps.containsKey(b.getAddress());

        subset=Arrays.asList(b.getAddress(), c.getAddress());
        rsps=disp.callRemoteMethods(subset, "foo", null, null, new RequestOptions(ResponseMode.GET_ALL, 0, false));
        System.out.println("rsps (no anycast): " + rsps);
        assert rsps.size() == 2;
        assert rsps.containsKey(b.getAddress());
        assert rsps.containsKey(c.getAddress());

        rsps=disp.callRemoteMethods(subset, "foo", null, null, new RequestOptions(ResponseMode.GET_ALL, 0, true));
        System.out.println("rsps (with anycast): " + rsps);
        assert rsps.size() == 2;
        assert rsps.containsKey(b.getAddress());
        assert rsps.containsKey(c.getAddress());
    }


    protected static class ServerObject {
        protected Address addr;

        public ServerObject(Address addr) {
            this.addr=addr;
        }

        public Address foo() {
            return addr;
        }

        public void setAddress(Address localAddress) {
            addr=localAddress;
        }
    }


 
}
