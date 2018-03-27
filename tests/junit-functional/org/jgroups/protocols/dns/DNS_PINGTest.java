package org.jgroups.protocols.dns;

import org.jgroups.stack.IpAddress;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DNS_PINGTest {

   private static final int PORT_START = 1234;

   private static final boolean FORCE_DEBUG_LOGGING = false;

   @BeforeClass
   public static void beforeClass() {
      if (FORCE_DEBUG_LOGGING) {
         Logger rootLog = Logger.getLogger("");
         rootLog.setLevel(Level.FINE);
         rootLog.getHandlers()[0].setLevel(Level.FINE);
      }
   }

   @Test
   public void test_failing_on_no_dns_query() throws Exception {
      //given
      DNS_PING ping = new DNS_PING();
      ping.dns_address = "fake.com";

      //when
      try {
         ping.validateProperties();
         Assert.fail();
      } catch (IllegalArgumentException e) {
         //then
      }
   }

   @Test
   public void test_valid_dns_response() throws Exception {
      //given
      DNSDiscoveryTester dns_discovery_tester = new DNSDiscoveryTester(2, PORT_START, 500, TimeUnit.SECONDS)
            .add("test.svc.cluster.local", DNSResolver.DNSRecordType.A, new IpAddress(InetAddress.getLoopbackAddress(), PORT_START))
            .add("test.svc.cluster.local", DNSResolver.DNSRecordType.A, new IpAddress(InetAddress.getLoopbackAddress(), PORT_START + 1));

      //when
      boolean was_view_received = dns_discovery_tester.runTestAndCheckIfViewWasReceived("test", "A");

      //then
      Assert.assertTrue(was_view_received);
   }

   @Test
   public void test_empty_dns_response() throws Exception {
      //given
      DNSDiscoveryTester dns_discovery_tester = new DNSDiscoveryTester(2, PORT_START, 1, TimeUnit.SECONDS);

      //when
      boolean was_view_received = dns_discovery_tester.runTestAndCheckIfViewWasReceived("test", "A");

      //then
      Assert.assertFalse(was_view_received);
   }

   @Test
   public void test_not_matching_dns_response() throws Exception {
      //given
      DNSDiscoveryTester dns_discovery_tester = new DNSDiscoveryTester(2, PORT_START, 1, TimeUnit.SECONDS)
            .add("test", DNSResolver.DNSRecordType.A, new IpAddress(InetAddress.getLoopbackAddress(), 6666));

      //when
      boolean was_view_received = dns_discovery_tester.runTestAndCheckIfViewWasReceived("test", "A");

      //then
      Assert.assertFalse(was_view_received);
   }

}