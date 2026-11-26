package org.jgroups.protocols.dns;

import org.jgroups.Address;

import java.util.*;

public class MockDNSResolverBuilder {

   private final Map<DNSResolverKey, List<Address>> resolutionMap = new HashMap<>();

   private MockDNSResolverBuilder() {

   }

   public static MockDNSResolverBuilder newDefault() {
      return new MockDNSResolverBuilder();
   }

   public MockDNSResolverBuilder add(String dnsRecord, DNSResolver.DNSRecordType recordType, Address address) {
      List<Address> physicalAddresses = resolutionMap.computeIfAbsent(new DNSResolverKey(dnsRecord, recordType), k -> new ArrayList<>());
      physicalAddresses.add(address);
      return this;
   }

   public DNSResolver build() {
      return (dnsQuery, recordType) -> resolutionMap.get(new DNSResolverKey(dnsQuery, recordType));
   }

   private static class DNSResolverKey {
      final String hostName;
      final DNSResolver.DNSRecordType recordType;

      public DNSResolverKey(String hostName, DNSResolver.DNSRecordType recordType) {
         this.hostName = hostName;
         this.recordType = recordType;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         DNSResolverKey that = (DNSResolverKey) o;

         if (!Objects.equals(hostName, that.hostName)) return false;
         return recordType == that.recordType;
      }

      @Override
      public int hashCode() {
         int result = hostName != null ? hostName.hashCode() : 0;
         result = 31 * result + (recordType != null ? recordType.hashCode() : 0);
         return result;
      }
   }
}
