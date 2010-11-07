package org.jgroups.blocks.mux;

/**
 * Allows registration/deregistrator of multiplexed handlers by mux id.
 * @author Paul Ferraro
 * @version $Id: Muxer.java,v 1.2 2010/06/09 03:24:51 bstansberry Exp $
 */
public interface Muxer<T> {

    /**
     * Registers the specified handler to handle messages containing a mux header with the specified mux identifier.
     * @param id a mux id
     * @param handler a handler for the specified id
     */
    void add(short id, T handler);
    
    /**
     * Gets the handler registered under the specified id
     * @param id a mux id
     * @return the handler, or <code>null</code> if no handler is registered under
     *         <code>id</code>
     */
    T get(short id);
    
    /**
     * Unregisters the handler associated with the specifed mux identifier
     * @param id a mux id
     */
    void remove(short id);
    
    /**
     * Gets the handler for messages that have no mux header.
     * 
     * @return the default handler, or <code>null</code> if no default handler
     *         has been set
     */
    T getDefaultHandler();
    
    /**
     * Sets the handler for messages that have no mux header.
     * 
     * @param handler a handler for messages that have no mux header
     */
    void setDefaultHandler(T handler);
}
