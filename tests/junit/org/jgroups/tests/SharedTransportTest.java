package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.conf.ProtocolData;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

/**
 * Tests which test the shared transport
 * @author Bela Ban
 * @version $Id: SharedTransportTest.java,v 1.2 2007/11/29 13:42:38 belaban Exp $
 */
public class SharedTransportTest extends ChannelTestBase {
    JChannel a, b, c, d, e;
    static final String SINGLETON_1="singleton-1", SINGLETON_2="singleton-2";


    protected void tearDown() throws Exception {
        if(e != null)
            e.close();
        if(d != null)
            d.close();
        if(c != null)
            c.close();
        if(b != null)
            b.close();
        if(a != null)
            a.close();
        super.tearDown();
    }


    public void testCreation() throws Exception {
        a=createChannel();
        makeSingleton(a, SINGLETON_1);
        a.connect("x");
        View view=a.getView();
        System.out.println("view = " + view);
        assertEquals(1, view.size());
    }

    public void testCreationOfDuplicateCluster() throws Exception {
//        ProtocolStackConfigurator config=ConfiguratorFactory.getStackConfigurator(CHANNEL_CONFIG);
//        ProtocolData[] protocols=config.getProtocolStack();
//        ProtocolData transport=protocols[0];
//        transport.getParameters().put(Global.SINGLETON_NAME, "bla");
        



        a=createChannel();
        makeSingleton(a, SINGLETON_1);

        a.connect("x");

        b=createChannel();
        makeSingleton(b, SINGLETON_1);

        b.connect("x");
    }



    private static void makeSingleton(JChannel channel, String singleton_name) {
//        ProtocolStack stack=channel.getProtocolStack();
//        Protocol transport=stack.getTransport();
//        transport.setProperty(Global.SINGLETON_NAME, singleton_name);
    }
}
