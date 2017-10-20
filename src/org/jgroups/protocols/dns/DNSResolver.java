package org.jgroups.protocols.dns;

import java.util.List;

import org.jgroups.Address;

public interface DNSResolver extends AutoCloseable {

    enum DNSRecordType {
        A, SRV
    }

    List<Address> resolveIps(String dnsQuery, DNSRecordType recordType);

    @Override
    default void close() {
        // Do nothing by default
    }
}
