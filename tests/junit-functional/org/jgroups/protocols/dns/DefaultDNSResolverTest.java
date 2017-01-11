package org.jgroups.protocols.dns;

import java.util.Arrays;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultDNSResolverTest {

   @Test
   public void test_parsing_a_entries() throws Exception {
      //given
      MockDirContext mockDirContext = MockDirContext.newDefault()
            .addEntry("test", "192.168.0.1", DNSResolver.DNSRecordType.A)
            .addEntry("test", "192.168.0.2", DNSResolver.DNSRecordType.A);

      DefaultDNSResolver resolver = new DefaultDNSResolver(mockDirContext);

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

      DefaultDNSResolver resolver = new DefaultDNSResolver(mockDirContext);

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
            .addEntry("test", "10 100 8888 9089f34a.jgroups-dns-ping.local.", DNSResolver.DNSRecordType.SRV)
            .addEntry("9089f34a.jgroups-dns-ping.local", "192.168.0.1", DNSResolver.DNSRecordType.A)
            .addEntry("9089f34a.jgroups-dns-ping.local", "192.168.0.2", DNSResolver.DNSRecordType.A);

      DefaultDNSResolver resolver = new DefaultDNSResolver(mockDirContext);

      //when
      List<Address> addresses = resolver.resolveIps("test", DNSResolver.DNSRecordType.SRV);

      //then
      List<Address> expectedResults = Arrays.asList(new IpAddress("192.168.0.1"), new IpAddress("192.168.0.2"));
      Assert.assertEquals(addresses, expectedResults);
   }

}