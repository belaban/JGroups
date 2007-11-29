package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

/**
 * Tests which test the shared transport
 * @author Bela Ban
 * @version $Id: SharedTransportTest.java,v 1.1 2007/11/29 12:53:33 belaban Exp $
 */
public class SharedTransportTest extends ChannelTestBase {




    
    private static void makeSingleton(JChannel channel, String singleton_name) {
        ProtocolStack stack=channel.getProtocolStack();
        Protocol transport=stack.getTransport();
        transport.setProperty(Global.SINGLETON_NAME, singleton_name);
    }
}
