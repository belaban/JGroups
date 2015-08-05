package org.jgroups.stack;

/**
 * Types of requests and responses exchanged between GossipRouter and RouterStubs
 * @author Bela Ban
 * @since  3.6.5
 */
public enum GossipType {
    REGISTER,
    UNREGISTER,
    GET_MBRS,
    GET_MBRS_RSP,
    MESSAGE,
    SUSPECT
}
