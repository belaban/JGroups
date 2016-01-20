package org.jgroups.blocks;

/**
 * Enum capturing the various response modes for RPCs
 * @author Bela Ban
 * @since 3.0
 */
public enum ResponseMode {
    /** Returns the first response received */
    GET_FIRST,

    /** return all responses */
    GET_ALL,

    /** return majority (of all non-faulty members) */
    @Deprecated /** use filter */
    GET_MAJORITY,

    /** return no response (async call) */
    GET_NONE
}
