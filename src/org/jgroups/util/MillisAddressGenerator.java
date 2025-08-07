package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.stack.AddressGenerator;

/**
 * Generates addresses with current time as key. Used for testing only!
 * @author Bela Ban
 * @since  5.5.0
 */
public class MillisAddressGenerator implements AddressGenerator {
    @Override
    public Address generateAddress() {
        return new MillisAddress(System.currentTimeMillis());
    }

    @Override
    public Address generateAddress(String ignored) {
        return new MillisAddress(System.currentTimeMillis());
    }
}
