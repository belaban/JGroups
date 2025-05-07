package org.jgroups;

import org.jgroups.util.UUID;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Represents a physical (as opposed to logical) address
 * 
 * @see UUID
 * @since 2.6
 * @author Bela Ban
 */
public interface PhysicalAddress extends Address {
    String                printIpAddress();
    default SocketAddress getSocketAddress() {return new InetSocketAddress(getIpAddress(), getPort());}
    InetAddress           getIpAddress();
    int                   getPort();
}
