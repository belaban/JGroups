package org.jgroups.protocols.dns;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.PingHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.BoundedList;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;
import org.jgroups.util.Tuple;

import java.util.List;

public class DNS_PING extends Discovery {

   private static final String DEFAULT_DNS_FACTORY = "com.sun.jndi.dns.DnsContextFactory";
   private static final String DEFAULT_DNS_RECORD_TYPE = "A";

   @Property(description = "DNS Context Factory")
   protected String dns_context_factory = DEFAULT_DNS_FACTORY;

   @Property(description = "DNS Address. This property will be assembed with the 'dns://' prefix")
   protected String dns_address;

   @Property(description = "DNS Record type")
   protected String dns_record_type = DEFAULT_DNS_RECORD_TYPE;

   @Property(description = "DNS query for fetching members")
   protected String dns_query;

   protected volatile DNSResolver dns_Resolver;

   protected BoundedList<Address> discovered_hosts = new BoundedList<>();

   private int transportPort;

   @Override
   public void init() throws Exception {
      super.init();
      validateProperties();
      transportPort = getTransport().getBindPort();
      if(transportPort <= 0) {
         log.warn("Unable to discover transport port. This may prevent members from being discovered.");
      }
      if(dns_Resolver == null) {
         dns_Resolver = new DefaultDNSResolver(dns_context_factory, dns_address);
      }
   }

   protected void validateProperties() {
      if(dns_query == null) {
         throw new IllegalArgumentException("dns_query can not be null or empty");
      }
      if(dns_address == null) {
         throw new IllegalArgumentException("dns_query can not be null or empty");
      }
   }

   @Override
   public boolean isDynamic() {
      return true;
   }

   @Override
   public Object down(Event evt) {
      Object retval = super.down(evt);
      switch (evt.getType()) {
         case Event.VIEW_CHANGE:
            for (Address logical_addr : view.getMembersRaw()) {
               PhysicalAddress physical_addr = (PhysicalAddress) down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, logical_addr));
               if (physical_addr != null) {
                  discovered_hosts.addIfAbsent(physical_addr);
               }
            }
            break;
         case Event.SUSPECT:
            Address address = evt.getArg();
            discovered_hosts.remove(address);
            break;
         case Event.ADD_PHYSICAL_ADDRESS:
            Tuple<Address, PhysicalAddress> tuple = evt.getArg();
            PhysicalAddress physical_addr = tuple.getVal2();
            if (physical_addr != null)
               discovered_hosts.addIfAbsent(physical_addr);
            break;
      }
      return retval;
   }

   public void discoveryRequestReceived(Address sender, String logical_name, PhysicalAddress physical_addr) {
      super.discoveryRequestReceived(sender, logical_name, physical_addr);
      log.debug("Received discovery from: %s, IP: %s", sender.toString(), physical_addr.printIpAddress());
      if (physical_addr != null)
         discovered_hosts.addIfAbsent(sender);
   }

   @Override
   public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
      PingData data = null;
      PhysicalAddress physical_addr = null;

      if (!use_ip_addrs || !initial_discovery) {
         log.debug("Performing initial discovery");
         physical_addr = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));

         // https://issues.jboss.org/browse/JGRP-1670
         data = new PingData(local_addr, false, NameCache.get(local_addr), physical_addr);
         if (members != null && members.size() <= max_members_in_discovery_request)
            data.mbrs(members);
      }

      List<Address> dns_discovery_members = dns_Resolver.resolveIps(dns_query, DNSResolver.DNSRecordType.valueOf(dns_record_type));
      log.debug("Entries collected from DNS: %s", dns_discovery_members);
      if(dns_discovery_members != null) {
         for(Address address : dns_discovery_members) {
            if (physical_addr != null && address.equals(physical_addr)) {
               // no need to send the request to myself
               continue;
            }

            Address addressToBeAdded = address;
            if(address instanceof IpAddress) {
               IpAddress ip = ((IpAddress) address);
               if(ip.getPort() == 0) {
                  log.debug("Discovered IP Address with port 0 (%s). Replacing with default Transport port: %d", ip.printIpAddress(), transportPort);
                  addressToBeAdded = new IpAddress(ip.getIpAddress(), transportPort);
               }
            }
            discovered_hosts.addIfAbsent(addressToBeAdded);
         }
      }

      log.debug("Performing discovery of the following hosts %s", discovered_hosts.toString());

      PingHeader hdr = new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name).initialDiscovery(initial_discovery);
      for (final Address addr : discovered_hosts) {


         // the message needs to be DONT_BUNDLE, see explanation above
         final Message msg = new Message(addr).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE, Message.Flag.OOB)
               .putHeader(this.id, hdr);
         if (data != null)
            msg.setBuffer(marshal(data));

         if (async_discovery_use_separate_thread_per_request)
            timer.execute(() -> sendDiscoveryRequest(msg), sends_can_block);
         else
            sendDiscoveryRequest(msg);
      }
   }

   protected void sendDiscoveryRequest(Message req) {
      try {
         log.debug("%s: sending discovery request to %s", local_addr, req.getDest());
         down_prot.down(req);
      } catch (Throwable t) {
         log.debug("sending discovery request to %s failed: %s", req.dest(), t);
      }
   }
}
