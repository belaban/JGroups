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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class DefaultDNSResolver implements DNSResolver {

    private static final Pattern SRV_REGEXP = Pattern.compile("\\d+ \\d+ \\d+ ([\\w+.-]+)\\.");

    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

    private final DirContext dnsContext;

    DefaultDNSResolver(DirContext context) {
        this.dnsContext = context;
    }

    public DefaultDNSResolver(String dnsContextFactory, String dnsAddress) throws NamingException {
        log.trace("Initializing DNS Context with factory: %s and url: %s", dnsContextFactory, dnsAddress);
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

        log.trace("Resolving DNS Query: %s of a type: %s", dnsQuery, recordType.toString());

        switch (recordType) {
            case A:
                return resolveAEntries(dnsQuery);
            case SRV:
                return resolveSRVEntries(dnsQuery);
            default:
                throw new IllegalStateException("Not implemented");
        }
    }

    private List<Address> resolveSRVEntries(String dnsQuery) {
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
                            String srcDNSRecord = matcher.group(1);
                            // The implementation here is not optimal but it's easy to read. SRV discovery will be performed
                            // extremely rare only when a fine grained discovery using ports is needed.
                            addresses.addAll(resolveAEntries(srcDNSRecord));
                        }
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

    private List<Address> resolveAEntries(String dnsQuery) {
        List<Address> addresses = new ArrayList<>();
        try {
            // We are parsing this kind of structure:
            // {a=A: 172.17.0.2, 172.17.0.7}
            // The frst attribute is the type of record. We are not interested in this. Next are addresses.
            Attributes attributes = dnsContext.getAttributes(dnsQuery, new String[] { DNSRecordType.A.toString() });
            if (attributes != null && attributes.getAll().hasMoreElements()) {
                NamingEnumeration<?> namingEnumeration = attributes.get(DNSRecordType.A.toString()).getAll();
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
