package org.jgroups.blocks;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.NotSerializableException;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class RpcDispatcherExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    JChannel channel;
    private final Target target=new Target();

    @BeforeClass void setUp() throws Exception {
        channel=createChannel();
        makeUnique(channel);
        disp=new RpcDispatcher(channel, target);
        channel.connect("RpcDispatcherExceptionTest");
    }

    @AfterClass void tearDown() throws Exception {
        Util.close(disp,channel);
    }


    public void testUnserializableValue() {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class},
                                   new RequestOptions(ResponseMode.GET_ALL, 5000));
            throw new IllegalStateException("this should have thrown an exception");
        }
        catch(Throwable t) {
            System.out.println("received an exception as expected: " + t);
            assert t instanceof NotSerializableException || t.getCause() instanceof NotSerializableException;
        }
    }

    public void testUnserializableValue2() {
        try {
            disp.callRemoteMethod(channel.getAddress(), "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class},
                                  new RequestOptions(ResponseMode.GET_ALL, 5000));
        }
        catch(Exception e) {
            System.out.println("received an exception as expected: " + e);
            assert e instanceof NotSerializableException || e.getCause() instanceof NotSerializableException;
        }
    }

    private static class Pojo { // doesn't implement Serializable !
        int age; String name;
    }

    private static class Target {
        public static void foo(Pojo p) {
            System.out.println(p.toString());
        }
    }



}
