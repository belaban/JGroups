package org.jgroups.protocols.dns;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

class AddressedDNSResolver extends DefaultDNSResolver {

    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

    AddressedDNSResolver(DirContext context) {
        super(context);
    }

    public AddressedDNSResolver(String dnsContextFactory, String dnsAddress) throws NamingException {
        super(dnsContextFactory, dnsAddress);
    }

    @Override
    protected List<Address> resolveAEntries(String dnsQuery) {
        List<Address> addresses = new ArrayList<>();
        try {
            // We are parsing this kind of structure:
            // {a=A: 172.17.0.2, 172.17.0.7}
            // The frst attribute is the type of record. We are not interested in this. Next are addresses.
            Attributes attributes = getDnsContext().getAttributes(dnsQuery, new String[] { DNSRecordType.A.toString() });
            if (attributes != null && attributes.getAll().hasMoreElements()) {
                NamingEnumeration<?> namingEnumeration = attributes.get(DNSRecordType.A.toString()).getAll();
                while (namingEnumeration.hasMoreElements()) {
                    try {
                        addresses.add(new IpAddress(namingEnumeration.nextElement().toString()));
                    } catch (Exception e) {
                        log.trace("non critical DNS resolution error", e);
                    }
                }
            }
        } catch (NamingException ex) {
            log.trace("no DNS records for query %s, ex: %a", dnsQuery, ex);
        }
        return addresses;
    }
}
