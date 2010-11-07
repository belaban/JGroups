// $Id: ChannelFactory.java,v 1.13 2009/07/07 06:09:03 belaban Exp $

package org.jgroups;

import org.w3c.dom.Element;

import java.io.File;
import java.net.URL;

/**
 * A channel factory that removes hardwiring of calls to create JGroups
 * channels. ChannelFactory enables client applications to use custom Channel
 * creation methodologies.
 * 
 * @see JChannelFactory
 * @deprecated Might get removed in 3.0. Use your own method of injecting channels
 */
@Deprecated
public interface ChannelFactory {

    /**
     * Initializes the factory.
     * 
     * 
     * @param properties
     * @throws ChannelException
     */
    void setMultiplexerConfig(Object properties) throws Exception;

    /**
     * Initializes the factory from a file. Example: conf/stacks.xml
     * 
     * @param properties
     * @throws ChannelException
     */
    void setMultiplexerConfig(File properties) throws Exception;

    void setMultiplexerConfig(Element properties) throws Exception;

    void setMultiplexerConfig(URL properties) throws Exception;

    void setMultiplexerConfig(String properties) throws Exception;

    /**
     * Creates an implementation of the Channel using a given stack name and
     * registering it under a given identity.
     * <p>
     * 
     * Channel has to be created with a unique application id per stack name.
     * 
     * <p>
     * Provided stack name has to be one of the stacks defined in a property
     * file that was passed to setMultiplexerConfig (e.g conf/stacks.xml). If
     * clients attempt to create a Channel for an undefined stack name or
     * they attempt to register a duplicate Channel per stack an Exception will be
     * thrown.
     * 
     * 
     * <p>
     * Rather than having each multiplexed channel do a separate state transfer
     * clients can bundle state transfers for all channels created with the same
     * ChannelFactory. First of all, clients have to create Channels with
     * register_for_state_transfer set to true. After the last Channel that was
     * created with register_for_state_transfer set to true connects and
     * initiates state transfer the actual state transfer for all such channels
     * from this ChannelFactory is executed.
     * 
     * <p>
     * Using bundled state transfers is especially useful with the FLUSH
     * protocol in a stack. Recall that each state transfer triggers a flush and
     * thus instead of doing a separate flush for each Channel created with this
     * ChannelFactory we execute only one flush for all state transfers.
     * 
     * <p>
     * However, be aware of the implication of asynchronous nature of bundled
     * state transfer with the respect of channel connect. Recall that each
     * Channel after it returns from successful getState method can assume that
     * state is available. In case of bundled state transfer, state will be set
     * only <strong>after</strong> the last Channel registered for the bundled
     * state transfer connects and executes getState.
     * 
     * 
     * 
     * 
     * @param stack_name
     *            The name of the stack to be used. All stacks are defined in
     *            the configuration with which the factory is configured (see
     *            {@link #setMultiplexerConfig(Object)} for example.
     * @param id
     *            The identifier used for multiplexing and demultiplexing
     *            (dispatching requests to one of possibly multiple receivers).
     *            Note that id needs to be a string since it will be shipped
     *            with each message. Try to pick a short string, because this is
     *            shipped with every message (overhead).
     * @param register_for_state_transfer
     *            If set to true, after all registered listeners called 
     *            either {@link Channel#connect(String, Address, String, long)} or 
     *            {@link Channel#connect(String) and Channel#getState(Address, long)} 
     *            successively on the returned Channel, the state for all 
     *            registered listeners will be fetched and set in all listeners.
     * @param substate_id
     *            The ID of the sub state to be retrieved. Set this to null if
     *            the entire state should be retrieved. If
     *            register_for_state_transfer is false, substate_id will be
     *            ignored
     *            
     *            
     * @return An implementation of Channel which keeps track of the id, so that
     *         it can be attached to each message and be properly dispatched at
     *         the receiver. 
     *         
     * @see Multiplexer
     * @see MuxChannel
     *         
     * @throws ChannelException
     */
    Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception;

    /**
     * Creates an implementation of the Channel using a given stack name and
     * registering it under a given identity.
     * <p>
     * 
     * Channel has to be created with a unique application id per stack name.
     * 
     * <p>
     * Provided stack name has to be one of the stacks defined in a property
     * file that was passed to setMultiplexerConfig (e.g conf/stacks.xml). If
     * clients attempt to create a Channel for an undefined stack name or
     * they attempt to register a duplicate Channel per stack an Exception will be
     * thrown.
     * 
     * 
     * 
     * @param stack_name
     *            The name of the stack to be used. All stacks are defined in
     *            the configuration with which the factory is configured (see
     *            {@link #setMultiplexerConfig(Object)} for example.
     * @param id
     *            The identifier used for multiplexing and demultiplexing
     *            (dispatching requests to one of possibly multiple receivers).
     *            Note that id needs to be a string since it will be shipped
     *            with each message. Try to pick a short string, because this is
     *            shipped with every message (overhead).
     *            
     * @return An implementation of Channel which keeps track of the id, so that
     *         it can be attached to each message and be properly dispatched at
     *         the receiver. 
     *         
     * @see Multiplexer
     * @see MuxChannel
     *         
     * @throws ChannelException
     */
    Channel createMultiplexerChannel(String stack_name, String id) throws Exception;

    Channel createChannel(Object props) throws ChannelException;

    /** Create a new channel with the properties defined in the factory */
    Channel createChannel() throws ChannelException;

    Channel createChannel(String stack_name) throws Exception;
}
