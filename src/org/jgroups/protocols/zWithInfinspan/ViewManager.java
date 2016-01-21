package org.jgroups.protocols.zWithInfinspan;


import org.jgroups.Address;
import org.jgroups.View;

import java.util.*;

/**
 * A class to provide a means of reducing a views size in bytes by storing each view in a byte array, and storing past view
 * data at each node in the cluster.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class ViewManager {
    private volatile View currentView;

    public void setCurrentView(View view) {
        this.currentView = view;
    }

    public boolean containsAddress(MessageOrderInfo messageOrderInfo, Address address) {
        return getDestinations(messageOrderInfo).contains(address);
    }

    public List<Address> getDestinations(MessageOrderInfo messageOrderInfo) {
        return getAddresses(currentView, messageOrderInfo.getDestinations());
    }

    public byte[] getDestinationsAsByteArray(Collection<Address> addresses) {
        if (addresses.size() > Byte.MAX_VALUE)
            throw new IllegalArgumentException("Number of addresses cannot be greater than " + Byte.MAX_VALUE);

        byte[] destinations = new byte[addresses.size()];
        int index = 0;
        for (Address address : addresses)
            destinations[index++] = (byte) currentView.getMembers().indexOf(address);
        return destinations;
    }

    public long getClientLastOrdering(MessageOrderInfo messageOrderInfo, Address address) {
        List<Address> destinations = getDestinations(messageOrderInfo);
        int addressIndex = destinations.indexOf(address);
        if (addressIndex >= 0)
            return messageOrderInfo.getclientsLastOrder()[addressIndex];
        else
            return -1;
    }

    private List<Address> getAddresses(View view, byte[] indexes) {
        if (view == null)
            throw new IllegalArgumentException("View cannot be null");

        List<Address> addresses = new ArrayList<Address>();
        for (byte index : indexes) {
            addresses.add(view.getMembers().get(index));
        }
        return addresses;
    }
}