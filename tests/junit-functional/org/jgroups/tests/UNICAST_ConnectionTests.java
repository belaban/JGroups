package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.ArrayList;

/**
 * Tests unilateral closings of UNICAST connections.
 * @author Bela Ban
 * @version $Id: UNICAST_ConnectionTests.java,v 1.2 2009/04/28 16:14:36 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class UNICAST_ConnectionTests {
    private JChannel c1, c2;
    private Address c1_addr, c2_addr;
    private MyReceiver r1=new MyReceiver("C1"), r2=new MyReceiver("C2");
    private static final String props="SHARED_LOOPBACK:UNICAST";
    private static final String CLUSTER="UNICAST_ConnectionTests";


    @BeforeMethod
    void start() throws Exception {
        c1=new JChannel(props);
        c1.connect(CLUSTER);
        c1_addr=c1.getAddress();
        c1.setReceiver(r1);
        c2=new JChannel(props);
        c2.connect(CLUSTER);
        c2_addr=c2.getAddress();
        c2.setReceiver(r2);
    }


    @AfterMethod void stop() {Util.close(c2, c1);}


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
