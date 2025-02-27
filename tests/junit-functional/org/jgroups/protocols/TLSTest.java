package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.TLSClientAuth;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;


/**
 * Tests that a rogue member (using a different keystore with a different certificate) cannot join a cluster with
 * a different keystore/certificate. We're using predefined keystore ./conf/keystore.jks and ./conf/rogue.jks.
 */
@Test(groups=Global.ENCRYPT,singleThreaded=true)
public class TLSTest {
   protected static final String KEYSTORE="keystore.jks";
   protected static final String KEYSTORE_PWD="password";
   protected static final String ROGUE_KEYSTORE="rogue.jks";
   protected JChannel            a, b, c, rogue;
   final String                  cluster_name = getClass().getSimpleName();


   @Test
   public void testTLS() throws Exception {
      a=create("A", KEYSTORE, "server").connect(cluster_name);
      b=create("B", KEYSTORE, "server").connect(cluster_name);
      c=create("C", KEYSTORE, "server").connect(cluster_name);
      Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);
      verifyForbiddenJoiner("rogue");
      Util.close(c, b, a);
   }

   private void verifyForbiddenJoiner(String name) throws Exception {
      rogue=create(name, ROGUE_KEYSTORE, "rogue");
      GMS gms=rogue.getProtocolStack().findProtocol(GMS.class);
      gms.setMaxJoinAttempts(1);
      try {
         rogue.connect(cluster_name);
      } catch (Exception ex) {
      }
      Util.waitUntilTrue(5000, 500, () -> a.getView().size() > 3);
      Arrays.asList(a, b, c).forEach(ch -> {
         View view=ch.getView();
         assert 3 == view.size();
         assert view.containsMembers(a.address(), b.address(), c.address());
         assert !view.containsMember(rogue.address());
      });
   }


   private static JChannel create(String name, String keystore, String alias) throws Exception {
      TCP tp=new TCP().setBindAddress(InetAddress.getLoopbackAddress()).setBindPort(9600);
      tp.tls().enabled(true).setClientAuth(TLSClientAuth.NEED).setKeystorePath(keystore)
        .setKeystorePassword(KEYSTORE_PWD).setKeystoreAlias(alias);
      TCPPING ping = new TCPPING()
        .setInitialHosts2(Collections.singletonList(new IpAddress(tp.getBindAddress(), tp.getBindPort())));
      return new JChannel(
        tp,
        ping,
        new NAKACK2(),
        new UNICAST3(),
        new STABLE(),
        new GMS())
        .name(name);
   }


}
