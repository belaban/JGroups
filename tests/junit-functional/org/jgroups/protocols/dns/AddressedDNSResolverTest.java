package org.jgroups.protocols.dns;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AddressedDNSResolverTest {

   @Test
   public void test_parsing_a_entries() throws Exception {
      //given
      MockDirContext mockDirContext = MockDirContext.newDefault()
            .addEntry("test", "192.168.0.1", DNSResolver.DNSRecordType.A)
            .addEntry("test", "192.168.0.2", DNSResolver.DNSRecordType.A);

      AddressedDNSResolver resolver = new AddressedDNSResolver(mockDirContext);

      //when
      List<Address> addresses = resolver.resolveIps("test", DNSResolver.DNSRecordType.A);

      //then
      List<Address> expectedResults = Arrays.asList(new IpAddress("192.168.0.1"), new IpAddress("192.168.0.2"));
      Assert.assertEquals(addresses, expectedResults);
   }

   @Test
   public void test_parsing_empty_response() throws Exception {
      //given
      MockDirContext mockDirContext = MockDirContext.newDefault();

      AddressedDNSResolver resolver = new AddressedDNSResolver(mockDirContext);

      //when
      List<Address> addresses = resolver.resolveIps("test", DNSResolver.DNSRecordType.A);

      //then
      Assert.assertTrue(addresses.isEmpty());
   }

   @Test
   public void test_parsing_srv_entries() throws Exception {
      //given
      MockDirContext mockDirContext = MockDirContext.newDefault()
            // This one is a bit weird - it seems that default DNS resolver implementation leaves the dot at the end.
            // Since we are tied to the implementation, we need to do the same (even though it's silly).
            .addEntry("test", "10 100 8888 9089f34a.jgroups-dns-ping.local", DNSResolver.DNSRecordType.SRV)
            .addEntry("test", "10 100 8888 9089f34b.jgroups-dns-ping.local", DNSResolver.DNSRecordType.SRV)
            .addEntry("9089f34a.jgroups-dns-ping.local", "192.168.1.16", DNSResolver.DNSRecordType.A)
            .addEntry("9089f34b.jgroups-dns-ping.local", "192.168.1.17", DNSResolver.DNSRecordType.A);

      AddressedDNSResolver resolver = new AddressedDNSResolver(mockDirContext);

      //when
      List<Address> addresses = resolver.resolveIps("test", DNSResolver.DNSRecordType.SRV);

      //then
      List<Address> expectedResults = Arrays.asList(new IpAddress("192.168.1.16:8888"), new IpAddress("192.168.1.17:8888"));
      Assert.assertEquals(addresses, expectedResults);
   }

   @Test
   public void test_srv_targets_are_resolved_through_configured_dns_context() throws Exception {
      //given
      MockDirContext mockDirContext = MockDirContext.newDefault()
             .addEntry("test", "10 100 8888 jgroups-dns-ping.invalid.", DNSResolver.DNSRecordType.SRV)
             .addEntry("jgroups-dns-ping.invalid.", "192.168.0.1", DNSResolver.DNSRecordType.A)
             .addEntry("jgroups-dns-ping.invalid.", "192.168.0.2", DNSResolver.DNSRecordType.A);

      AddressedDNSResolver resolver = new AddressedDNSResolver(mockDirContext);

      //when
      List<Address> srvAddresses = resolver.resolveIps("test", DNSResolver.DNSRecordType.SRV);

      //then
      List<Address> expectedResults = Arrays.asList(new IpAddress("192.168.0.1:8888"), new IpAddress("192.168.0.2:8888"));
      Assert.assertEquals(srvAddresses, expectedResults);
   }

}