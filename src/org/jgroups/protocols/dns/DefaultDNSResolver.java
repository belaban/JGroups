package org.jgroups.protocols.dns;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

class DefaultDNSResolver implements DNSResolver {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   final String dnsContextFactory;
   final String dnsAddress;
   final DirContext dnsContext;

   public DefaultDNSResolver(String dnsContextFactory, String dnsAddress) {
      this.dnsContextFactory = dnsContextFactory;
      this.dnsAddress = dnsAddress;
      dnsContext = getDnsContext();
   }

   private final DirContext getDnsContext() {
      try {
         log.trace("Initializing DNS Context with factory: %s and url: %s" + dnsContextFactory, dnsAddress);
         Hashtable env = new Hashtable();
         env.put("java.naming.factory.initial", dnsContextFactory);
         env.put("java.naming.provider.url", "dns://" + dnsAddress);
         return new InitialDirContext(env);
      } catch (NamingException e) {
         throw new IllegalStateException("Wrong DNS Context", e);
      }
   }


   @Override
   public List<Address> resolveIps(String dnsQuery, DNSRecordType recordType) {
      List<Address> addresses = new ArrayList<>();
      log.trace("Resolving DNS Query: %s of a type: %s", dnsQuery, recordType.toString());

      try {
         // We are parsing this kind of structure:
         // {a=A: 172.17.0.2, 172.17.0.7}
         // The frst attribute is the type of record. We are not interested in this. Next are addresses.
         Attributes attributes = dnsContext.getAttributes(dnsQuery, new String[]{recordType.toString()});
         if(attributes != null && attributes.getAll().hasMoreElements()) {
            NamingEnumeration<?> namingEnumeration = attributes.get(recordType.toString()).getAll();
            while (namingEnumeration.hasMoreElements()) {
               try {
                  addresses.add(new IpAddress(namingEnumeration.nextElement().toString()));
               } catch (Exception e) {
                  log.trace("Non critical DNS resolution error", e);
                  continue;
               }
            }
         }
      } catch (NamingException e) {
         log.trace("No DNS records for query: " + dnsQuery);
      }

      return addresses;
   }
}
