package org.jgroups;

import org.jgroups.util.UUID;

/**
 * Represents a physical (as opposed to logical) address
 * 
 * @see UUID
 * @since 2.6
 * @author Bela Ban
 */
public interface PhysicalAddress extends Address {
    String printIpAddress();
}
