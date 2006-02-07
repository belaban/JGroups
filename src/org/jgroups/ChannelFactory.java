// $Id: ChannelFactory.java,v 1.3 2006/02/07 08:02:25 belaban Exp $

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
    void config(Object properties) throws ChannelException;

    /**
     * Initializes the factory from  a file. Example: conf/stacks.xml
     * @param properties
     * @throws ChannelException
     */
    void config(File properties) throws ChannelException;

    void config(Element properties) throws ChannelException;

    void config(URL properties) throws ChannelException;

    void config(String properties) throws ChannelException;

    /**
     * Creates an implementation of Channel using a guven stack name and registering under a given identity.
     * The latter is used for multiplexing requests to and from a block on top of a channel.
     * @param stack_name The name of the stack to be used. All stacks are defined in the configuration
     * with which the factory is configured (see {@link #config(Object)} for example.
     * @param receiver The receiver (see {@link Receiver} for details
     * @param id The identifier used for multiplexing and demultiplexing (dispatching requests to one of possibly
     * multiple receivers). Note that id will most probably be a number or a string, but since it will be shipped
     * with the message it needs to be serializable (we also support the {@link org.jgroups.util.Streamable}
     * interface.
     * @return An implementation of Channel which keeps track of the id, so that it can be attached to each message
     * and be properly dispatched at the receiver
     * @throws ChannelException
     */
    // Channel createChannel(String stack_name, Receiver receiver, Object id) throws ChannelException;

    Channel createChannel(Object props) throws ChannelException;
}
