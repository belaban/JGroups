package org.jgroups.protocols.dns;

import java.util.List;

import org.jgroups.Address;

public interface DNSResolver {

    enum DNSRecordType {
        A, SRV
    }

    List<Address> resolveIps(String dnsQuery, DNSRecordType recordType);

}
