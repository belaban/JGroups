package org.jgroups.protocols.dns;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;

public class DNSDiscoveryTester {

   private final MockDNSResolverBuilder dnsResolverBuilder = MockDNSResolverBuilder.newDefault();
   private final int numberOfTestedInstances;
   private final int timeout;
   private final TimeUnit unit;
   private final int portStart;

   public DNSDiscoveryTester(int numberOfTestedInstances, int portStart, int timeout, TimeUnit unit) {
      this.numberOfTestedInstances = numberOfTestedInstances;
      this.timeout = timeout;
      this.unit = unit;
      this.portStart = portStart;
   }

   public DNSDiscoveryTester add(String dnsRecord, DNSResolver.DNSRecordType recordType, Address address) {
      dnsResolverBuilder.add(dnsRecord, recordType, address);
      return this;
   }

   public boolean runTestAndCheckIfViewWasReceived(String dnsQuery, String recordType) throws Exception {
      List<JChannel> channels = new ArrayList<>();

      CountDownLatch waitForViewToForm = new CountDownLatch(1);

      for(int i = 0; i < numberOfTestedInstances; ++i) {
         DNS_PING ping = new DNS_PING();
         ping.dns_Resolver = dnsResolverBuilder.build();
         ping.dns_query = dnsQuery;
         ping.dns_record_type = recordType;
         ping.dns_address = "fake.com";

         Protocol[] protocols={
               new TCP().setValue("bind_addr", InetAddress.getLoopbackAddress()).setValue("bind_port", portStart + i),
               ping,
               new NAKACK2(),
               new UNICAST3(),
               new STABLE(),
               new GMS().joinTimeout(timeout)
         };

         JChannel c = new JChannel(protocols).name(UUID.randomUUID().toString());
         channels.add(c);

         c.setReceiver(new ReceiverAdapter() {
            @Override
            public void viewAccepted(View view) {
               if(view.getMembers().size() == numberOfTestedInstances) {
                  waitForViewToForm.countDown();
               }
            }
         });

         c.connect("TEST");
      }

      boolean viewReceived = waitForViewToForm.await(timeout, unit);
      channels.forEach(JChannel::close);

      return viewReceived;
   }

   private int findUnusedPort() throws IOException {
      ServerSocket s = new ServerSocket(0);
      int port = s.getLocalPort();
      s.close();
      return port;
   }
}
