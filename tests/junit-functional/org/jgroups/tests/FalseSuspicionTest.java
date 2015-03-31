package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.InetAddress;

/**
 * Tests false suspicions and UNSUSPECT events (https://issues.jboss.org/browse/JGRP-1922)
 * @author Bela Ban
 * @since  3.6.4
 */
@Test(singleThreaded=true)
public class FalseSuspicionTest {
    protected static final int    NUM=5;
    protected static final Method suspect_method;
    protected JChannel[]          channels=new JChannel[NUM];

    static {
        try {
            suspect_method=FD_SOCK.class.getDeclaredMethod("broadcastSuspectMessage", Address.class);
            suspect_method.setAccessible(true);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeMethod protected void setup() throws Exception {
        for(int i=0; i < channels.length; i++) {
            channels[i]=create(String.valueOf(i+1));
            channels[i].connect("FalseSuspicionTest");
        }
    }

    @AfterMethod protected void destroy() {Util.close(channels);}


    public void testFalseSuspicion() throws Exception {
        JChannel e=channels[channels.length-1];
        FD_SOCK fd_sock=(FD_SOCK)e.getProtocolStack().findProtocol(FD_SOCK.class);

        System.out.println("*** suspecting " + channels[1].getAddress());
        suspect_method.invoke(fd_sock, channels[1].getAddress());

        for(int i=0; i < 10; i++) {
            if(getTotalSuspectedMembers() == 0)
                break;
            Util.sleep(1000);
        }
        assert getTotalSuspectedMembers() == 0;
    }


    protected int getTotalSuspectedMembers() {
        int total=0;
        for(JChannel ch: channels) {
            FD_SOCK fd_sock=(FD_SOCK)ch.getProtocolStack().findProtocol(FD_SOCK.class);
            total+=fd_sock.getNumSuspectedMembers();
        }
        return total;
    }



    protected JChannel create(String name) throws Exception {
        return new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new FD_SOCK().setValue("bind_addr", InetAddress.getByName("127.0.0.1")),
          new VERIFY_SUSPECT(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000)).name(name);
    }
}
