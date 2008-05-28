package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.Global;
import org.jgroups.ChannelException;
import org.jgroups.util.Util;
import org.jgroups.protocols.TP;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

/**
 * @author Bela Ban
 * @version $Id: SetPropertyTest.java,v 1.1 2008/05/28 09:17:12 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class SetPropertyTest {
    JChannel ch;

    @BeforeClass
    void init() throws ChannelException {
        ch=new JChannel();
    }

    @AfterClass
    void destroy() {
        Util.close(ch);
    }


    public void testSetter() {
        TP transport=ch.getProtocolStack().getTransport();
        int port=transport.getBindPort();
        System.out.println("port = " + port);
        transport.setBindPort(port +20);
        int old_port=port;
        port=transport.getBindPort();
        System.out.println("port = " + port);
        assert old_port + 20 == port;

        transport.setProperty("bind_port", "10000");
        port=transport.getBindPort();
        System.out.println("port = " + port);
        assert port == 10000;
    }
}
