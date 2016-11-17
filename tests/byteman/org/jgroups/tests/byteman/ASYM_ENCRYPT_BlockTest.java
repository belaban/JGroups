package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.auth.MD5Token;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests https://issues.jboss.org/browse/JGRP-2131
 * @author Bela Ban
 * @since  3.6.12, 4.0.0
 */
@Test(groups=Global.BYTEMAN,singleThreaded=true)
public class ASYM_ENCRYPT_BlockTest extends BMNGRunner {
    protected JChannel a, b;
    protected MyReceiver ra, rb;

    @BeforeMethod protected void setup() throws Exception {
        a=create("A");
        b=create("B");
        a.setReceiver(ra=new MyReceiver());
        b.setReceiver(rb=new MyReceiver());
        a.connect(ASYM_ENCRYPT_BlockTest.class.getSimpleName());
        b.connect(ASYM_ENCRYPT_BlockTest.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);
    }

    @AfterMethod protected void tearDown() {
        Util.close(b, a);
    }

    @BMScript(dir="scripts/ASYM_ENCRYPT_BlockTest", value="testASYM_ENCRYPT_NotBlockingJoin")
    public void testASYM_ENCRYPT_NotBlockingJoin() throws Exception {
        a.send(b.getAddress(), "one");
        b.send(a.getAddress(), "two");


        for(int i=0; i < 10; i++) {
            if(ra.size() >= 1 && rb.size() >= 1) // fail fast if size > 1
                break;
            Util.sleep(1000);
        }

        System.out.printf("A's messages:\n%s\nB's messages:\n%s\n", print(ra), print(rb));
        assert ra.size() == 1 : String.format("A has %d messages", ra.size());
        assert rb.size() >= 1 : String.format("B has %d messages", rb.size());
        boolean match=false;
        for(Object obj: rb.list()) {
            if(obj.equals("one")) {
                match=true;
                break;
            }
        }
        assert match;
        assert "two".equals(ra.list().get(0));
    }


    protected JChannel create(String name) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new ASYM_ENCRYPT().encryptEntireMessage(false).symKeylength(128)
            .symAlgorithm("AES/ECB/PKCS5Padding").asymKeylength(512).asymAlgorithm("RSA"),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new AUTH().setAuthToken(new MD5Token("jdgservercluster", "MD5")),
          new GMS().joinTimeout(5000),
          new FRAG2().fragSize(8000)
        };

        return new JChannel(protocols).name(name);
    }

    protected static String print(MyReceiver r) {
        StringBuilder sb=new StringBuilder();
        for(Object str: r.list())
            sb.append(str + "\n");
        return sb.toString();
    }
}
