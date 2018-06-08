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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;

/**
 * Tests graceful leaving of the coordinator and second-in-line in a 10 node cluster with ASYM_ENCRYPT configured
 * @author Bela Ban
 * @since  4.0.12
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ASYM_ENCRYPT_LeaveTest {
    // protected static final String CONFIG="asym-ssl.xml";
    protected static final String      KEYSTORE="my-keystore.jks";
    protected static final String      KEYSTORE_PWD="password";
    protected static final int         NUM=10;
    protected static final int         BIND_PORT=7600;
    protected static final InetAddress LOOPBACK;

    static {
        try {
            LOOPBACK=Util.getLocalhost();
        }
        catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }




    protected JChannel[] channels=new JChannel[NUM];

    @BeforeMethod
    protected void setup() throws Exception {
        for(int i=0; i < channels.length; i++) {
            channels[i]=create(String.valueOf(i+1)).connect(ASYM_ENCRYPT_LeaveTest.class.getSimpleName());
            if(i == 0)
                Util.sleep(2000);
            else Util.sleep(500);
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

    @AfterMethod
    protected void destroy() {
        for(int i=channels.length-2; i >= 0; i--)
            channels[i].close();
    }


    public void testGracefulLeave() throws Exception {
        for(int j=0; j < channels.length; j++) {
            System.out.printf("%-4s: view is %s\n", channels[j].getAddress(), channels[j].getView());
        }
        System.out.println("\n");

        JChannel[] remaining_channels=new JChannel[channels.length-2];
        System.arraycopy(channels, 2, remaining_channels, 0, channels.length-2);

        // Util.close(channels[0], channels[1]);

        new Thread(() -> {
            Util.sleep(500);
            Util.close(channels[1]);
        }).start();
        Util.close(channels[0]);


        Util.waitUntilAllChannelsHaveSameView(10000, 1000, remaining_channels);
        for(int i=0; i < remaining_channels.length; i++)
            System.out.printf("%-4s: view is %s\n", remaining_channels[i].getAddress(), remaining_channels[i].getView());
    }




    /** Creates a channel with a config similar to ./conf/asym-ssl.xml */
    protected static JChannel create(String name) throws Exception {
        // return new JChannel(CONFIG).name(name);
        return new JChannel(
          new TCP().setBindAddress(LOOPBACK).setBindPort(BIND_PORT),
          new TCPPING().portRange(10).initialHosts(Collections.singleton(new InetSocketAddress(LOOPBACK, BIND_PORT))),
          new MERGE3(),
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
