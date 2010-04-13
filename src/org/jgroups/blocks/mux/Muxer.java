package org.jgroups.blocks.mux;

/**
 * Allows registration/deregistrator of multiplexed handlers by mux id.
 * @author Paul Ferraro
 * @version $Id: Muxer.java,v 1.1 2010/04/13 17:57:07 ferraro Exp $
 */
public interface Muxer<T> {

    /**
     * Registers the specified handler to handle messages containing a mux header with the specified mux identifier.
     * @param id a mux id
     * @param handler a handler for the specified id
     */
    void add(short id, T handler);
    
    /**
     * Unregisters the handler associated with the specifed mux identifier
     * @param id a mux id
     */
    void remove(short id);
}
