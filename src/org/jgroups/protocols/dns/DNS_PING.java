package org.jgroups.protocols.dns;

import org.jgroups.*;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.PingHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArray;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.jgroups.Message.Flag.*;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

public class DNS_PING extends Discovery {

    private static final String DEFAULT_DNS_FACTORY = "com.sun.jndi.dns.DnsContextFactory";
    private static final String DEFAULT_DNS_RECORD_TYPE = "A";

    @Property(description = "DNS Context Factory.  Used when DNS_PING is configured to use SRV record types and when using A types with a specific dns_address.")
    protected String  dns_context_factory = DEFAULT_DNS_FACTORY;

    @Property(description = "DNS Address. This property will be assembled with the 'dns://' prefix.  If this is specified, A records will be resolved through DnsContext.")
    protected String  dns_address = "";

    @Property(description = "DNS Record type",
            systemProperty="jgroups.dns.dns_record_type")
    protected String  dns_record_type = DEFAULT_DNS_RECORD_TYPE;

    @Property(description = "A comma-separated list of DNS queries for fetching members",
      systemProperty="jgroups.dns.dns_query")
    protected String  dns_query;

    @Property(description="For SRV records returned by the DNS query, the non-0 ports returned by DNS are " +
      "used. If this attribute is true, then the transport ports will also be used. Ignored for A records.")
    protected boolean probe_transport_ports;



    protected volatile DNSResolver dns_resolver;

    private int                    transportPort, portRange;

    @Override
    public void init() throws Exception {
        super.init();
        validateProperties();
        transportPort = getTransport().getBindPort();
        portRange = getTransport().getPortRange();
        if (dns_resolver == null) {
            if (dns_address == null || dns_address.isEmpty()) {
                dns_resolver = new DefaultDNSResolver(dns_context_factory, dns_address);
            } else {
                dns_resolver = new AddressedDNSResolver(dns_context_factory, dns_address);
            }
        }
    }

    public DNSResolver dnsResolver() { return dns_resolver; }
    public DNS_PING dnsResolver(DNSResolver r) { dns_resolver = r; return this; }

    public DNS_PING setDNSResolver(DNSResolver r) { dns_resolver = r; return this; }
    public DNSResolver getDNSResolver(DNSResolver r) { return dns_resolver; }

    public String dnsQuery() { return dns_query; }
    public DNS_PING dnsQuery(String q) { dns_query = q; return this; }

    public DNS_PING setDNSQuery(String q) { dns_query = q; return this; }
    public String getDNSQuery() { return dns_query; }

    public String dnsRecordType() { return dns_record_type; }
    public DNS_PING dnsRecordType(String t) { dns_record_type = t; return this; }

    public String dnsAddress() { return dns_address; }
    public DNS_PING dnsAddress(String a) { dns_address = a; return this; }

    public String getDNSAddress() { return this.dns_address; }
    public DNS_PING setDNSAddress(String a) { dns_address = a; return this; }

    public String dnsContextFactory() { return dns_context_factory; }
    public DNS_PING dnsContextFactory(String f) { dns_context_factory = f; return this; }

    public String getDNSContextFactory() { return dns_context_factory; }
    public DNS_PING setDNSContextFactory(String f) { dns_context_factory = f; return this; }

    protected void validateProperties() {
        if (dns_query == null || dns_query.trim().isEmpty()) {
            throw new IllegalArgumentException("dns_query can not be null or empty");
        }
    }

    @Override
    public void destroy() {
        if (dns_resolver != null) {
            dns_resolver.close();
        }
    }

    @Override
    public boolean isDynamic() {
        return true;
    }

    /**
     * Gets a list of IP addresses for the provided DNS query list and record type.
     *
     * @param dns_query A comma-separated list of DNS queries. Must not be {@code null}.
     * @param dns_record_type The DNS record type.
     *
     * @return A list of IP addresses corresponding to the provided DNS query list and record type. An empty list is
     * returned if the provided DNS query does not resolve to any IP addresses.
     *
     * @throws NullPointerException if dns_query is {@code null}.
     */
    List<Address> getMembers(final String dns_query, final DNSResolver.DNSRecordType dns_record_type) {
        final List<Address> dns_discovery_members = new ArrayList<>();
        for (final String query : dns_query.split(",")) {
            final List<Address> addresses = dns_resolver.resolveIps(query.trim(), dns_record_type);
            if (addresses != null) {
                dns_discovery_members.addAll(addresses);
            }
        }
        return dns_discovery_members;
    }

    @ManagedOperation(description="Executes the DNS query and returns the result in string format")
    public String fetchFromDns() {
        long start=System.currentTimeMillis();
        List<Address> dns_discovery_members = getMembers(dns_query, DNSResolver.DNSRecordType.valueOf(dns_record_type));
        String ret=dns_discovery_members != null && !dns_discovery_members.isEmpty() ? dns_discovery_members.toString() : null;
        long time=System.currentTimeMillis()-start;
        return String.format("%s\n(took %d ms)\n", ret, time);
    }


    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        PingData                  data = null;
        PhysicalAddress           physical_addr = null;
        Set<PhysicalAddress>      cluster_members=new LinkedHashSet<>();
        DNSResolver.DNSRecordType record_type=DNSResolver.DNSRecordType.valueOf(dns_record_type);

        physical_addr = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        // https://issues.redhat.com/browse/JGRP-1670
        data = new PingData(local_addr, false, NameCache.get(local_addr), physical_addr);
        if (members != null && members.size() <= max_members_in_discovery_request)
            data.mbrs(members);

        long start=System.currentTimeMillis();
        List<Address> dns_discovery_members = getMembers(dns_query, record_type);
        long time=System.currentTimeMillis()-start;
        if(log.isDebugEnabled()) {
            if(dns_discovery_members != null && !dns_discovery_members.isEmpty())
                log.debug("%s: entries collected from DNS (in %d ms): %s", local_addr, time, dns_discovery_members);
            else
                log.debug("%s: no entries collected from DNS (in %d ms)", local_addr, time);
        }

        boolean ports_found=false;
        if (dns_discovery_members != null) {
            for (Address address : dns_discovery_members) {
                if(address instanceof IpAddress) {
                    IpAddress ip = ((IpAddress) address);
                    if(record_type == DNSResolver.DNSRecordType.SRV && ip.getPort() > 0) {
                        ports_found=true;
                        cluster_members.add(ip);
                        if(!probe_transport_ports)
                            continue;
                    }
                    for(int i=0; i <= portRange; i++)
                        cluster_members.add(new IpAddress(ip.getIpAddress(), transportPort + i));
                }
            }
            cluster_members.remove(physical_addr); // skip request to self (https://issues.redhat.com/browse/JGRP-2669)
        }

        if(dns_discovery_members != null && !dns_discovery_members.isEmpty() && log.isDebugEnabled()) {
            if(ports_found)
                log.debug("%s: sending discovery requests to %s", local_addr, cluster_members);
            else
                log.debug("%s: sending discovery requests to hosts %s on ports [%d .. %d]",
                          local_addr, dns_discovery_members, transportPort, transportPort + portRange);
        }

        ByteArray data_buf=data != null? marshal(data) : null;
        PingHeader hdr = new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name).initialDiscovery(initial_discovery);
        for (Address addr: cluster_members) {

            // the message needs to be DONT_BUNDLE, see explanation above
            final Message msg = new BytesMessage(addr).setFlag(DONT_BUNDLE, OOB).setFlag(DONT_LOOPBACK)
              .putHeader(this.id, hdr);
            if (data_buf != null)
                msg.setArray(data_buf);

            if (async_discovery_use_separate_thread_per_request)
                timer.execute(() -> sendDiscoveryRequest(msg), sends_can_block);
            else
                sendDiscoveryRequest(msg);
        }
    }

    protected void sendDiscoveryRequest(Message req) {
        try {
            log.trace("%s: sending discovery request to %s", local_addr, req.getDest());
            down_prot.down(req);
        } catch (Throwable t) {
            log.error("sending discovery request to %s failed: %s", req.dest(), t);
        }
    }
}
