package org.jgroups.fork;

import org.jgroups.*;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of a ForkChannel, which is a light-weight channel. Not all methods are supported,
 * UnsupportedOperationExceptions will be thrown if an unsupported operation is called.
 * See doc/design/FORK.txt for details
 * @author Bela Ban
 * @since  3.4
 */
public class ForkChannel extends JChannel implements ChannelListener {
    protected final JChannel          main_channel;
    protected final String            fork_channel_id;
    protected static final Field[]    copied_fields;

    static {
        String[] fields={"state", "local_addr", "name", "cluster_name", "view"};
        copied_fields=new Field[fields.length];
        for(int i=0; i < fields.length; i++) {
            Field field=Util.getField(JChannel.class, fields[i]);
            if(field == null)
                throw new IllegalStateException("field \"" + fields[i] + "\" not found in JChannel");
            copied_fields[i]=field;
        }
    }


    /**
     * Creates a new fork-channel from a main-channel. The channel is unconnected and {@link ForkChannel#connect(String)}
     * needs to be called to send and receive messages.
     * @param main_channel The main-channel. The lifetime of the newly created channel will be less than or equal to
     *                     the main-channel
     * @param fork_stack_id The ID to associate the fork-stack with in FORK
     * @param fork_channel_id The ID used to map fork-channel IDs to ForkChannels in the fork-channels protocol stack
     * @param create_fork_if_absent If true, and FORK doesn't exist, a new FORK protocol will be created and inserted
     *                              into the main-stack at the given position. If false, and FORK doesn't exist, an
     *                              exception will be thrown
     * @param position The position at which the newly created FORK will be inserted. {@link ProtocolStack.Position#ABOVE} or
     *                 {@link ProtocolStack.Position#BELOW} are accepted. Ignored if create_fork_if_absent is false.
     * @param neighbor The class of the neighbor protocol below or above which the newly created FORK protocol will
     *                 be inserted. Ignored if create_fork_if_absent is false.
     * @param protocols A list of protocols (<em>from bottom to top</em> !) to insert as the fork_stack in FORK under the
     *                  given fork_stack_id. If the fork-stack with fork_stack_id already exists, an exception will be
     *                  thrown.
     *                  Can be null if no protocols should be added. This may be the case when an app only wants to use
     *                  a ForkChannel to mux/demux messages, but doesn't need a different protocol stack.
     *
     * @throws Exception
     */
    public ForkChannel(final JChannel main_channel, String fork_stack_id, String fork_channel_id,
                       boolean create_fork_if_absent, ProtocolStack.Position position, Class<? extends Protocol> neighbor,
                       Protocol ... protocols) throws Exception {

        super(false);
        if(main_channel == null)    throw new IllegalArgumentException("main channel cannot be null");
        if(fork_stack_id == null)   throw new IllegalArgumentException("fork_stack_id cannot be null");
        if(fork_channel_id == null) throw new IllegalArgumentException("fork_channel_id cannot be null");

        this.main_channel=main_channel;
        this.fork_channel_id=fork_channel_id;

        FORK fork;
        // To prevent multiple concurrent FORK creations https://issues.jboss.org/browse/JGRP-1842
        synchronized(this.main_channel) {
            fork=getFORK(main_channel, position, neighbor, create_fork_if_absent);
        }

        // Returns the existing fork stack for fork_stack_id, or creates a new one
        prot_stack=fork.createForkStack(fork_stack_id, protocols == null? null : Arrays.asList(protocols), true);
        flush_supported=main_channel.flushSupported();
        state=State.OPEN;
    }

    /**
     * Creates a new fork-channel from a main-channel. The channel is unconnected and {@link ForkChannel#connect(String)}
     * needs to be called to send and receive messages. If FORK is not found in the stack, an exception will be thrown.
     * @param main_channel The main-channel. The lifetime of the newly created channel will be less than or equal to
     *                     the main-channel
     * @param fork_stack_id The ID to associate the fork-stack with in FORK
     * @param fork_channel_id The ID used to map fork-channel IDs to ForkChannels in the fork-channels protocol stack
     * @param protocols A list of protocols (<em>from bottom to top</em> !) to insert as the fork_stack in FORK under the
     *                  given fork_stack_id. If the fork-stack with fork_stack_id already exists, an exception will be
     *                  thrown.
     *                  Can be null if no protocols should be added. This may be the case when an app only wants to use
     *                  a ForkChannel to mux/demux messages, but doesn't need a different protocol stack.
     * @throws Exception
     */
    public ForkChannel(final JChannel main_channel, String fork_stack_id, String fork_channel_id,
                       Protocol ... protocols) throws Exception {
        this(main_channel, fork_stack_id, fork_channel_id, false, ProtocolStack.Position.ABOVE, null, protocols);
    }

    @Override
    public ForkChannel setName(String name) {
        log.error("name (%s) cannot be set in a fork-channel", name);
        return this;
    }

    @Override
    public JChannel name(String name) {
        log.error("name (%s) cannot be set in a fork-channel", name);
        return this;
    }

    @Override public void channelConnected(JChannel channel) {
        copyFields();
        if(local_addr == null) return;
        Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
        if(up_handler != null)
            up_handler.up(evt);
    }

    @Override public void channelDisconnected(JChannel channel) {
        copyFields();
    }

    @Override public void channelClosed(JChannel channel) {
        copyFields();
    }

    /**
     * Connects the fork-channel, which will be operational after this. Note that the fork-channel will
     * have the same state as the main-channel, ie. if the main-channel is disconnected, so will the fork-channel be,
     * even if connect() was called. This connect() method essentially adds the fork-channel to the fork-stack's hashmap,
     * ready to send/receive messages as soon as the main-channel has been connected.<p/>
     * This method does <em>not</em> affect the main-channel.
     * @param cluster_name Ignored, will be the same as the main-channel's cluster name
     * @throws Exception
     */
    @Override
    public ForkChannel connect(String cluster_name) throws Exception {
        if(!this.main_channel.isConnected())
            throw new IllegalStateException("main channel is not connected");
        if(state == State.CONNECTED)
            return this;
        if(state == State.CLOSED)
            throw new IllegalStateException("a closed fork channel cannot reconnect");

        state=State.CONNECTING;
        this.main_channel.addChannelListener(this);
        copyFields();
        JChannel existing_ch=((ForkProtocolStack)prot_stack).putIfAbsent(fork_channel_id,this);
        if(existing_ch != null && existing_ch != this)
            throw new IllegalArgumentException("fork-channel with id=" + fork_channel_id + " is already present");
        setLocalAddress(local_addr);

        prot_stack.startStack();

        prot_stack.down(new Event(Event.CONNECT, cluster_name));

        View current_view=main_channel.getView();
        if(current_view != null) {
            up(new Event(Event.VIEW_CHANGE, current_view));
            prot_stack.down(new Event(Event.VIEW_CHANGE, current_view));
        }
        state=State.CONNECTED;
        notifyChannelConnected(this);
        return this;
    }

    @Override
    public ForkChannel connect(String cluster_name, Address target, long timeout) throws Exception {
        connect(cluster_name);
        main_channel.getState(target, timeout);
        return this;
    }

    /** Removes the fork-channel from the fork-stack's hashmap and resets its state. Does <em>not</em> affect the
     * main-channel */
    @Override
    public ForkChannel disconnect() {
        if(state != State.CONNECTED)
            return this;
        prot_stack.down(new Event(Event.DISCONNECT, local_addr)); // will be discarded by ForkProtocol
        prot_stack.stopStack(cluster_name);
        ((ForkProtocolStack)prot_stack).remove(fork_channel_id);
        nullFields();
        state=State.OPEN;
        notifyChannelDisconnected(this);
        return this;
    }

    /** Closes the fork-channel, essentially setting its state to CLOSED. Note that - contrary to a regular channel -
     * a closed fork-channel can be connected again: this means re-attaching the fork-channel to the main-channel*/
    @Override
    public void close() {
        ((ForkProtocolStack)prot_stack).remove(fork_channel_id);
        if(state == State.CLOSED)
            return;
        disconnect();  // leave cluster if connected
        prot_stack.destroy();
        state=State.CLOSED;
        notifyChannelClosed(this);
    }

    @Override
    public Object down(Message msg) {
        setHeader(msg);
        return super.down(msg);
    }

    @Override
    public ForkChannel send(Message msg) throws Exception {
        checkClosedOrNotConnected();
        FORK.ForkHeader hdr=msg.getHeader(FORK.ID);
        if(hdr != null)
            hdr.setForkChannelId(fork_channel_id);
        else {
            hdr=new FORK.ForkHeader(null, fork_channel_id);
            msg.putHeader(FORK.ID, hdr);
        }
        prot_stack.down(msg);
        return this;
    }

    @Override
    public ForkChannel startFlush(List<Address> flushParticipants, boolean automatic_resume) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ForkChannel startFlush(boolean automatic_resume) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ForkChannel stopFlush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ForkChannel stopFlush(List<Address> flushParticipants) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ForkChannel getState(Address target, long timeout) throws Exception {
        main_channel.getState(target, timeout);
        return this;
    }

    @Override
    public ForkChannel addAddressGenerator(AddressGenerator address_generator) {
        main_channel.addAddressGenerator(address_generator);
        return this;
    }

    protected ForkChannel setLocalAddress(Address local_addr) {
        if(local_addr != null) {
            Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
            ((ForkProtocolStack)prot_stack).setLocalAddress(local_addr); // sets the address only in the protocols managed by the fork-prot-stack
            if(up_handler != null)
                up_handler.up(evt);
        }
        return this;
    }


    /**
     * Creates a new FORK protocol, or returns the existing one, or throws an exception. Never returns null.
     */
    protected static FORK getFORK(JChannel ch, ProtocolStack.Position position, Class<? extends Protocol> neighbor,
                                  boolean create_fork_if_absent) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        FORK fork=stack.findProtocol(FORK.class);
        if(fork == null) {
            if(!create_fork_if_absent)
                throw new IllegalArgumentException("FORK not found in main stack");
            fork = new FORK();
            fork.setProtocolStack(stack);
            stack.insertProtocol(fork, position, neighbor);
        }
        return fork;
    }



    protected void setHeader(Message msg) {
        FORK.ForkHeader hdr=msg.getHeader(FORK.ID);
        if(hdr != null)
            hdr.setForkChannelId(fork_channel_id);
        else
            msg.putHeader(FORK.ID, new FORK.ForkHeader(null, fork_channel_id));
    }


    /** Copies state from main-channel to this fork-channel */
    protected void copyFields() {
        for(Field field: copied_fields) {
            Object value=Util.getField(field,main_channel);
            Util.setField(field, this, value);
        }
    }

    protected void nullFields() {
        for(Field field: copied_fields)
            Util.setField(field, this, null);
    }


}
