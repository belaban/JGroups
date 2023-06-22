package org.jgroups.stack;

import org.jgroups.Address;

/**
 * Callback to provide custom addresses. Will be called by {@link org.jgroups.JChannel#connect(String)}.
 * @author Bela Ban
 * @since 2.12
 */
public interface AddressGenerator {
    @Deprecated(since="5.2.15")
    Address generateAddress();
    default Address generateAddress(String name) {
        return generateAddress();
    }
}
