// $Id: ChannelFactory.java,v 1.9 2006/09/01 14:40:27 belaban Exp $

package org.jgroups;

import org.w3c.dom.Element;

import java.io.File;
import java.net.URL;

/**
   A channel factory takes care of creation of channel implementations. Subclasses will create
   different implementations.
 */
public interface ChannelFactory {

    /**
     * Initializes the factory.
     * @param properties
     * @throws ChannelException
     */
    void setMultiplexerConfig(Object properties) throws Exception;

    /**
     * Initializes the factory from  a file. Example: conf/stacks.xml
     * @param properties
     * @throws ChannelException
     */
    void setMultiplexerConfig(File properties) throws Exception;

    void setMultiplexerConfig(Element properties) throws Exception;

    void setMultiplexerConfig(URL properties) throws Exception;

    void setMultiplexerConfig(String properties) throws Exception;

    /**
     * Creates an implementation of Channel using a guven stack name and registering under a given identity.
     * The latter is used for multiplexing requests to and from a block on top of a channel.
     * @param stack_name The name of the stack to be used. All stacks are defined in the configuration
     * with which the factory is configured (see {@link #setMultiplexerConfig(Object)} for example.
     * @param id The identifier used for multiplexing and demultiplexing (dispatching requests to one of possibly
     * multiple receivers). Note that id needs to be a string since it will be shipped with each message. Try to pick
     * a short string, because this is shipped with every message (overhead). todo: possibly change to short ?
     * @param register_for_state_transfer If set to true, after all registered listeners called connect() on the returned Channel,
     * the state for all registered listeners will be fetched and set in all listeners
     * @param substate_id The ID of the substate to be retrieved. Set this to null if the entire state should be retrieved. If
     * register_for_state_transfer is false, substate_id will be ignored
     * @return An implementation of Channel which keeps track of the id, so that it can be attached to each message
     * and be properly dispatched at the receiver. This will be a {@link org.jgroups.mux.MuxChannel}.
     * @throws ChannelException
     */
    Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception;

    Channel createMultiplexerChannel(String stack_name, String id) throws Exception;

    Channel createChannel(Object props) throws ChannelException;

    /** Create a new channel with the properties defined in the factory */
    Channel createChannel() throws ChannelException;
}
