package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.stream.Stream;

/**
 * Tests graceful leaving of the coordinator and second-in-line in a 10 node cluster with ASYM_ENCRYPT configured<br/>
 * Reproducer for https://issues.jboss.org/browse/JGRP-2297
 * @author Bela Ban
 * @since  4.0.12
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ASYM_ENCRYPT_LeaveTest {
    protected static final String      KEYSTORE="my-keystore.jks";
    protected static final String      KEYSTORE_PWD="password";
    protected static final int         NUM=10;
    protected static final int         NUM_LEAVERS=2;
    protected static final InetAddress LOOPBACK;

    static {
        try {
            LOOPBACK=Util.getLocalhost();
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }




    protected JChannel[] channels=new JChannel[NUM];

    @BeforeMethod
    protected void setup() throws Exception {
        for(int i=0; i < channels.length; i++) {
            channels[i]=create(String.valueOf(i+1)).connect(ASYM_ENCRYPT_LeaveTest.class.getSimpleName());
            Util.sleep(i == 0? 2000 : 1000);
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

    @AfterMethod
    protected void destroy() {
        for(int i=channels.length-NUM_LEAVERS; i >= 0; i--)
            channels[i].close();
    }


    public void testGracefulLeave() throws Exception {
        for(int j=0; j < channels.length; j++) {
            System.out.printf("%-4s: view is %s\n", channels[j].getAddress(), channels[j].getView());
        }
        System.out.println("\n");

        JChannel[] remaining_channels=new JChannel[channels.length-NUM_LEAVERS];
        System.arraycopy(channels, NUM_LEAVERS, remaining_channels, 0, channels.length-NUM_LEAVERS);

        Stream.of(channels).limit(NUM_LEAVERS).forEach(Util::close);
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, remaining_channels);
        for(int i=0; i < remaining_channels.length; i++)
            System.out.printf("%-4s: view is %s\n", remaining_channels[i].getAddress(), remaining_channels[i].getView());
    }




    /** Creates a channel with a config similar to ./conf/asym-ssl.xml */
    protected static JChannel create(String name) throws Exception {
        return new JChannel(
          new TCP().setBindAddress(LOOPBACK), // .setBindPort(BIND_PORT),
          new MPING(), // new TCPPING().portRange(10).initialHosts(Collections.singleton(new InetSocketAddress(LOOPBACK, BIND_PORT))),
          // omit MERGE3 from the stack -- nodes are leaving gracefully
          //new MERGE3().setMinInterval(2000).setMaxInterval(5000),
          new FD_SOCK(),
          new FD_ALL(),
          new VERIFY_SUSPECT(),
          new SSL_KEY_EXCHANGE().setKeystoreName(KEYSTORE).setKeystorePassword(KEYSTORE_PWD).setPortRange(10),
          new ASYM_ENCRYPT().setUseExternalKeyExchange(true).setChangeKeyOnLeave(true)
            .symKeylength(128).symAlgorithm("AES").asymKeylength(512).asymAlgorithm("RSA"),
          new NAKACK2().setUseMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000))
          .name(name);
    }

}
