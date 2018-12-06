package org.jgroups.protocols.dns;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class DefaultDNSResolver implements DNSResolver {

    private static final Pattern SRV_REGEXP = Pattern.compile("\\d+ \\d+ (\\d+) ([\\w+\\.-]+)");

    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

    private final DirContext dnsContext;

    DefaultDNSResolver(DirContext context) {
        this.dnsContext = context;
    }

    public DefaultDNSResolver(String dnsContextFactory, String dnsAddress) throws NamingException {
        log.trace("initializing DNS Context with factory: %s and url: %s", dnsContextFactory, dnsAddress);
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, dnsContextFactory);
        if (dnsAddress != null) {
            env.put(Context.PROVIDER_URL, "dns://" + dnsAddress);
        }
        this.dnsContext = new InitialDirContext(env);
    }

    @Override
    public void close() {
        try {
            this.dnsContext.close();
        } catch (NamingException e) {
            log.warn(e.getLocalizedMessage(), e);
        }
    }

    @Override
    public List<Address> resolveIps(String dnsQuery, DNSRecordType recordType) {

        log.trace("resolving DNS query: %s of a type: %s", dnsQuery, recordType.toString());

        switch (recordType) {
            case A:
                return resolveAEntries(dnsQuery);
            case SRV:
                return resolveSRVEntries(dnsQuery);
            default:
                throw new IllegalStateException("Not implemented");
        }
    }

    protected DirContext getDnsContext() {
        return dnsContext;
    }

    protected List<Address> resolveSRVEntries(String dnsQuery) {
        List<Address> addresses = new ArrayList<>();
        try {
            // We are parsing this kind of structure:
            // {srv=SRV: 10 100 8888 9089f34a.jgroups-dns-ping.myproject.svc.cluster.local.}
            // The frst attribute is the type of record. We are not interested in this. Next are addresses.
            Attributes attributes = dnsContext.getAttributes(dnsQuery, new String[] { DNSRecordType.SRV.toString() });
            if (attributes != null && attributes.getAll().hasMoreElements()) {
                NamingEnumeration<?> namingEnumeration = attributes.get(DNSRecordType.SRV.toString()).getAll();
                while (namingEnumeration.hasMoreElements()) {
                    try {
                        String srvEntry = namingEnumeration.nextElement().toString();
                        Matcher matcher = SRV_REGEXP.matcher(srvEntry);
                        if (matcher.find()) {
                            String srcPort = matcher.group(1);
                            String srcDNSRecord = matcher.group(2);
                            // The implementation here is not optimal but it's easy to read. SRV discovery will be performed
                            // extremely rarely, only when a fine grained discovery using ports is needed (ie: when using containers).
                            addresses.addAll(resolveAEntries(srcDNSRecord, srcPort));
                        }
                    } catch (Exception e) {
                        log.trace("non critical DNS resolution error", e);
                    }
                }
            }
        } catch (NamingException ex) {
            log.trace("no DNS records for query %s, ex: %s", dnsQuery, ex.getMessage());
        }

        return addresses;
    }

    protected List<Address> resolveAEntries(String dnsQuery) {
        return resolveAEntries(dnsQuery, "0");
    }

    protected static List<Address> resolveAEntries(String dnsQuery, String srcPort) {
        List<Address> addresses = new ArrayList<>();
        try {
            InetAddress[] inetAddresses = InetAddress.getAllByName(dnsQuery);
            for (InetAddress address : inetAddresses) {
                addresses.add(new IpAddress(address, Integer.parseInt(srcPort)));
            }
        } catch (UnknownHostException ex) {
            log.trace("failed to resolve query %s, ex: %s", dnsQuery, ex.getMessage());
        }
        return addresses;
    }
}
