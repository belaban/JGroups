package org.jgroups.protocols;


import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.Util;

/**
 * Tests for contention on UNICAST, measured by the number of retransmissions in UNICAST 
 * @author Bela Ban
 * @version $Id: UNICAST_ContentionTest.java,v 1.1.2.1 2009/09/14 07:57:44 belaban Exp $
 */
public class UNICAST_ContentionTest extends TestCase {
    JChannel c1, c2;

    static final String props="SHARED_LOOPBACK:UNICAST";

    protected void tearDown() throws Exception {
        Util.close(c2, c1);
        super.tearDown();
    }


    public static void testSimpleMessageReception() throws Exception {
        JChannel c1=new JChannel(props);
        JChannel c2=new JChannel(props);
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        c1.connect("testSimpleMessageReception");
        c2.connect("testSimpleMessageReception");
        View view=c2.getView();
        System.out.println("view = " + view);
        assert view.size() == 2 : "view: " + view + ", expecting 2 members";

        int NUM=100;
        Address c1_addr=c1.getLocalAddress(), c2_addr=c2.getLocalAddress();
        for(int i=1; i <= NUM; i++) {
            c1.send(c1_addr, null, "bla");
            c1.send(c2_addr, null, "bla");
            c2.send(c2_addr, null, "bla");
            c2.send(c1_addr, null, "bla");
        }

        for(int i=0; i < 10; i++) {
            if(r1.getNum() == NUM * 2 && r2.getNum() == NUM * 2)
                break;
            Util.sleep(500);
        }

        System.out.println("c1 received " + r1.getNum() + " msgs, c2 received " + r2.getNum() + " msgs");

        assert r1.getNum() == NUM * 2 : "expected " + NUM *2 + ", but got " + r1.getNum();
        assert r2.getNum() == NUM * 2 : "expected " + NUM *2 + ", but got " + r2.getNum();
    }











    private static class MyReceiver extends ReceiverAdapter {
        int num=0;

        public void receive(Message msg) {
            num++;
        }

        public int getNum() {
            return num;
        }
    }




}