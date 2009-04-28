package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.UNICAST;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.ArrayList;

/**
 * Tests unilateral closings of UNICAST connections. The test scenarios are described in doc/design.UNICAST.new.txt.
 * @author Bela Ban
 * @version $Id: UNICAST_ConnectionTests.java,v 1.3 2009/04/28 16:33:50 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class UNICAST_ConnectionTests {
    private JChannel c1, c2;
    private Address c1_addr, c2_addr;
    private MyReceiver r1=new MyReceiver("C1"), r2=new MyReceiver("C2");
    private UNICAST u1, u2;
    private static final String props="SHARED_LOOPBACK:UNICAST";
    private static final String CLUSTER="UNICAST_ConnectionTests";


    @BeforeMethod
    void start() throws Exception {
        c1=new JChannel(props);
        c1.connect(CLUSTER);
        c1_addr=c1.getAddress();
        c1.setReceiver(r1);
        u1=(UNICAST)c1.getProtocolStack().findProtocol(UNICAST.class);
        c2=new JChannel(props);
        c2.connect(CLUSTER);
        c2_addr=c2.getAddress();
        c2.setReceiver(r2);
        u2=(UNICAST)c2.getProtocolStack().findProtocol(UNICAST.class);
    }


    @AfterMethod void stop() {Util.close(c2, c1);}


    /**
     * Tests cases #1 and #2 of UNICAST.new.txt
     * @throws Exception
     */
    public void testRegularMessageReception() throws Exception {
        for(int i=0; i < 100; i++)
            c1.send(c2_addr, null, "msg #" + i);
        Util.sleep(500);
        int size=r2.size();
        System.out.println("size = " + size);
        assert size == 100;

        for(int i=0; i < 50; i++)
            c2.send(c1_addr, null, "msg #" + i);
        size=r1.size();
        System.out.println("size = " + size);
        assert size == 50;
    }


    /**
     * Tests case #3 of UNICAST.new.txt
     */
    public void testBothChannelsClosing() throws Exception {
        for(int i=1; i <= 10; i++) {
            c1.send(c2_addr, null, "m" + i);
            c2.send(c1_addr, null, "m" + i);
        }
        List<Message> l1=r1.getMessages();
        List<Message> l2=r2.getMessages();
        Util.sleep(500);
        System.out.println("l1 = " + print(l1));
        System.out.println("l2 = " + print(l2));

        // now close the connections to each other
        System.out.println("==== Closing the connections on both sides");
        u1.removeConnection(c2_addr);
        u2.removeConnection(c1_addr);
        r1.clear(); r2.clear();

        // causes new connection establishment
        for(int i=11; i <= 20; i++) {
            c1.send(c2_addr, null, "m" + i);
            c2.send(c1_addr, null, "m" + i);
        }
        l1=r1.getMessages();
        l2=r2.getMessages();
        Util.sleep(500);
        System.out.println("l1 = " + print(l1));
        System.out.println("l2 = " + print(l2));
        assert l1.size() == 10;
        assert l2.size() == 10;
    }

    private static String print(List<Message> list) {
        List<String> tmp=new ArrayList<String>(list.size());
        for(Message msg: list)
            tmp.add((String)msg.getObject());
        return Util.printListWithDelimiter(tmp, " ");
    }


    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        final List<Message> msgs=new ArrayList<Message>(20);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            msgs.add(msg);
        }

        public List<Message> getMessages() { return msgs; }
        public void clear() {msgs.clear();}
        public int size() {return msgs.size();}

        public String toString() {
            return name;
        }
    }
}
