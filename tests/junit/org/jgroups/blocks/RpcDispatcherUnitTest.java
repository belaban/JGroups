package org.jgroups.blocks;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.Address;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.*;
import org.testng.annotations.*;

import java.util.List;
import java.util.Arrays;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class RpcDispatcherUnitTest extends ChannelTestBase {
    private RpcDispatcher d1, d2, d3;
    private JChannel      c1, c2, c3;
    private ServerObject  o1, o2, o3;
    private Address       a1, a2, a3;
    private List<Address> members;


    @BeforeClass
    protected void setUp() throws Exception {
        o1=new ServerObject();
        o2=new ServerObject();
        o3=new ServerObject();

        c1=createChannel(true, 3);
        c1.setName("A");
        final String GROUP="RpcDispatcherUnitTest";
        d1=new RpcDispatcher(c1, null, null, o1);
        c1.connect(GROUP);

        c2=createChannel(c1);
        c2.setName("B");
        d2=new RpcDispatcher(c2, null, null, o2);
        c2.connect(GROUP);

        c3=createChannel(c1);
        c3.setName("C");
        d3=new RpcDispatcher(c3, null, null, o3);
        c3.connect(GROUP);

        System.out.println("c1.view=" + c1.getView() + "\nc2.view=" + c2.getView() + "\nc3.view=" + c3.getView());
        View view=c3.getView();
        assert view.size() == 3 : "view=" + view;

        a1=c1.getAddress();
        a2=c2.getAddress();
        a3=c3.getAddress();
        members=Arrays.asList(a1, a2, a3);
    }

    @BeforeMethod
    protected void reset() {
        o1.reset();
        o2.reset();
        o3.reset();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        d3.stop();
        d2.stop();
        d1.stop();
        Util.close(c3, c2, c1);
    }


    public void testInvocationOnEntireGroup() {
        RspList rsps=d1.callRemoteMethods(null, "foo", null, null, RequestOptions.SYNC);
        System.out.println("rsps:\n" + rsps);
        assert rsps.size() == 3;
        assert o1.wasCalled() && o2.wasCalled() && o3.wasCalled();
    }

    public void testInvocationOnEntireGroupWithTargetList() {
        RspList rsps=d1.callRemoteMethods(members, "foo", null, null, RequestOptions.SYNC);
        System.out.println("rsps:\n" + rsps);
        assert rsps.size() == 3;
        assert o1.wasCalled() && o2.wasCalled() && o3.wasCalled();
    }


    /** Invoke a method on all but myself */
    public void testInvocationWithExclusionOfSelf() {
        RequestOptions options=new RequestOptions(Request.GET_ALL, 5000).setExclusionList(a1);
        RspList rsps=d1.callRemoteMethods(null, "foo", null, null, options);
        Util.sleep(500);
        System.out.println("rsps:\n" + rsps);
        assert rsps.size() == 2;
        assert rsps.containsKey(a2) && rsps.containsKey(a3);
        assert !o1.wasCalled() && o2.wasCalled() && o3.wasCalled();
    }

    public void testInvocationWithExclusionOfTwo() {
        RequestOptions options=new RequestOptions(Request.GET_ALL, 5000).setExclusionList(a2, a3);
        RspList rsps=d1.callRemoteMethods(null, "foo", null, null, options);
        Util.sleep(500);
        System.out.println("rsps:\n" + rsps);
        assert rsps.size() == 1;
        assert rsps.containsKey(a1);
        assert o1.wasCalled() && !o2.wasCalled() && !o3.wasCalled();
    }

    public void testInvocationOnEmptyTargetSet() {
        RequestOptions options=new RequestOptions(Request.GET_ALL, 5000).setExclusionList(a1, a2, a3);
        RspList rsps=d1.callRemoteMethods(null, "foo", null, null, options);
        Util.sleep(500);
        System.out.println("rsps:\n" + rsps);
        assert rsps.isEmpty();
        assert !o1.wasCalled() && !o2.wasCalled() && !o3.wasCalled();
    }



    private static class ServerObject {
        boolean called=false;

        public boolean wasCalled() {
            return called;
        }

        public void reset() {
            called=false;
        }

        public boolean foo() {
            called=true;
            return called;
        }
    }



}