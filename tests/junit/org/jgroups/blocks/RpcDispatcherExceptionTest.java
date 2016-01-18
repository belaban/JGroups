package org.jgroups.blocks;


import org.jgroups.AbstractChannel;
import org.jgroups.Global;
import org.jgroups.tests.ChannelTestBase;
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
    AbstractChannel channel;
    private final Target target=new Target();

    @BeforeClass
    void setUp() throws Exception {
        channel=createChannel(true);
        disp=new RpcDispatcher(channel, target);
        channel.connect("RpcDispatcherExceptionTest");
    }

    @AfterClass
    void tearDown() throws Exception {
        disp.stop();
        channel.close();
    }


    public void testUnserializableValue() {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class},
                                   new RequestOptions(ResponseMode.GET_ALL, 5000));
            throw new IllegalStateException("this should have thrown an exception");
        }
        catch(Throwable t) {
            System.out.println("received an exception as expected: " + t);
            assert t instanceof NotSerializableException;
        }
    }

    // @Test(expectedExceptions=NotSerializableException.class)
    public void testUnserializableValue2() {
        try {
            disp.callRemoteMethod(channel.getAddress(), "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class},
                                  new RequestOptions(ResponseMode.GET_ALL, 5000));
        }
        catch(Exception e) {
            System.out.println("received an exception as expected: " + e);
            assert e instanceof NotSerializableException;
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
