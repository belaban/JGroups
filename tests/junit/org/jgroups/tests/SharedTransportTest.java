package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolData;
import org.jgroups.conf.ProtocolParameter;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests which test the shared transport
 * @author Bela Ban
 * @version $Id: SharedTransportTest.java,v 1.6 2008/01/24 07:51:56 belaban Exp $
 */
public class SharedTransportTest extends ChannelTestBase {
    private JChannel a, b, c;
    private MyReceiver r1, r2, r3;
    static final String SINGLETON_1="singleton-1", SINGLETON_2="singleton-2";


    protected void tearDown() throws Exception {
        if(c != null)
            c.close();
        if(b != null)
            b.close();
        if(a != null)
            a.close();
        r1=r2=r3=null;
        super.tearDown();
    }


    public void testCreationNonSharedTransport() throws Exception {
        a=createChannel();
        a.connect("x");
        View view=a.getView();
        System.out.println("view = " + view);
        assertEquals(1, view.size());
    }

    public void testCreationOfDuplicateCluster() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_1);
        a.connect("x");
        try {
            b.connect("x");
            fail("b should not be able to join cluster 'x' as a has already joined it");
        }
        catch(Exception ex) {
            System.out.println("b was not able to join the same cluster (\"x\") as expected");
        }
    }

    public void testView() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_2);
        a.setReceiver(new MyReceiver(SINGLETON_1));
        b.setReceiver(new MyReceiver(SINGLETON_2));

        a.connect("x");
        b.connect("x");

        View view=a.getView();
        assertEquals(2, view.size());
        view=b.getView();
        assertEquals(2, view.size());
    }

    public void testView2() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_1);
        a.setReceiver(new MyReceiver("first-channel"));
        b.setReceiver(new MyReceiver("second-channel"));

        a.connect("x");
        b.connect("y");

        View view=a.getView();
        assertEquals(1, view.size());
        view=b.getView();
        assertEquals(1, view.size());
    }

    public void testCreationOfDifferentCluster() throws Exception {
        a=createSharedChannel(SINGLETON_1);
        b=createSharedChannel(SINGLETON_2);
        a.connect("x");
        b.connect("x");
        View view=b.getView();
        System.out.println("b's view is " + view);
        assertEquals(2, view.size());
    }


    public void testReferenceCounting() throws ChannelException {
        a=createSharedChannel(SINGLETON_1);
        r1=new MyReceiver("a");
        a.setReceiver(r1);

        b=createSharedChannel(SINGLETON_1);
        r2=new MyReceiver("b");
        b.setReceiver(r2);

        c=createSharedChannel(SINGLETON_1);
        r3=new MyReceiver("c");
        c.setReceiver(r3);

        a.connect("A");
        b.connect("B");
        c.connect("C");

        a.send(null, null, "message from a");
        b.send(null, null, "message from b");
        c.send(null, null, "message from c");
        Util.sleep(500);
        assertEquals(1, r1.size());
        assertEquals(1, r2.size());
        assertEquals(1, r3.size());
        r1.clear(); r2.clear(); r3.clear();

        b.disconnect();
        System.out.println("\n");
        a.send(null, null, "message from a");
        c.send(null, null, "message from c");
        Util.sleep(500);
        assertEquals(1, r1.size());
        assertEquals(1, r3.size());
        r1.clear(); r3.clear();

        c.disconnect();
        System.out.println("\n");
        a.send(null, null, "message from a");
        Util.sleep(500);
        assertEquals(1, r1.size());
    }



    private static JChannel createSharedChannel(String singleton_name) throws ChannelException {
        ProtocolStackConfigurator config=ConfiguratorFactory.getStackConfigurator(CHANNEL_CONFIG);
        ProtocolData[] protocols=config.getProtocolStack();
        ProtocolData transport=protocols[0];
        transport.getParameters().put(Global.SINGLETON_NAME, new ProtocolParameter(Global.SINGLETON_NAME, singleton_name));
        return new JChannel(config);
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<Message> list=new LinkedList<Message>();
        final String name;

        private MyReceiver(String name) {
            this.name=name;
        }

        public List<Message> getList() {
            return list;
        }

        public int size() {
            return list.size();
        }

        public void clear() {
            list.clear();
        }

        public void receive(Message msg) {
            System.out.println("[" + name + "]: received message from " + msg.getSrc() + ": " + msg.getObject());
            list.add(msg);
        }

        public void viewAccepted(View new_view) {
            System.out.println("view = " + new_view);
        }
    }

}
