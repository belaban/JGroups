package org.jgroups.blocks;


import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.NotSerializableException;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherExceptionTest.java,v 1.5 2008/04/08 07:44:55 belaban Exp $
 */
public class RpcDispatcherExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    Channel channel;

    @BeforeMethod
    public void setUp() throws Exception {
        channel=createChannel("A");
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect("demo");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        disp.stop();
        channel.close();
    }

    public void foo(Pojo p) {
        System.out.println(p.toString());
    }

    @Test
    public void testUnserializableValue() {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class}, GroupRequest.GET_ALL, 5000);
            throw new IllegalStateException("this should have thrown an exception");
        }
        catch(Throwable t) {
            System.out.println("received an exception as expected: " + t);
            assertTrue(t.getCause() instanceof NotSerializableException);
        }
    }

    @Test(expectedExceptions=NotSerializableException.class)
    public void testUnserializableValue2() throws Throwable {
        disp.callRemoteMethod(channel.getLocalAddress(), "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class},
                              GroupRequest.GET_ALL, 5000);
    }

    private static class Pojo { // doesn't implement Serializable !
        int age; String name;
    }



}
