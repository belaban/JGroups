package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.stack.AddressGenerator;

/**
 * Implementation of {@link org.jgroups.stack.AddressGenerator} which is configured with an initial value, and
 * after that random values are generated.
 * @author Bela Ban
 * @since  3.5
 */
public class OneTimeAddressGenerator implements AddressGenerator {
    protected final long initial_val;
    protected boolean    first=true;

    public OneTimeAddressGenerator(long initial_val) {
        this.initial_val=initial_val;
    }

    public Address generateAddress() {
        if(first) {
            first=false;
            return new UUID(0, initial_val);
        }
        return Util.createRandomAddress();
    }
}
