package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;

/**
 * Tests https://issues.jboss.org/browse/JGRP-1973
 * @author Bela Ban
 * @since  3.6.7
 */
@Test(groups=Global.FUNCTIONAL)
public class FragTest2 {
    protected JChannel a, b;
    protected static final StackType type=Util.getIpStackType();
    protected static final String bind_addr=type == StackType.IPv6 ? "::1" : "127.0.0.1";
    protected static final String MSG="Hello world from Bela Ban";

    @BeforeMethod protected void setup() throws Exception {
        a=create("A");
        a.connect("FragTest2");
        b=create("B");
        b.connect("FragTest2");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
    }

    @AfterMethod protected void destroy() {Util.close(b, a);}


    /* Tests https://issues.jboss.org/browse/JGRP-1973 */
    public void testFragCorruption() throws Exception {
        byte[] buf=MSG.getBytes();
        MyReceiver r=new MyReceiver();
        b.setReceiver(r);
        a.send(new Message(null, buf).setFlag(Message.Flag.OOB));
        for(int i=0; i < 10; i++) {
            String msg=r.msg();
            if(msg != null) {
                assert msg.equals(MSG) : String.format("expected \"%s\" but received \"%s\"\n", MSG, msg);
                System.out.printf("received \"%s\"\n", msg);
                break;
            }
            Util.sleep(500);
        }
    }



    protected JChannel create(String name) throws Exception {
        return new JChannel(
          new UDP().setValue("bind_addr", InetAddress.getByName(bind_addr)),
          new PING(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000),
          new FRAG2().fragSize(20)
          // new FRAG().setValue("frag_size", 20)
        ).name(name);
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected String msg;
        public String msg() {return msg;}
        public void receive(Message msg) {
            this.msg=new String(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
        }
    }
}
