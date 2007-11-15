package org.jgroups.mux;

import org.jgroups.*;
import org.jgroups.stack.ProtocolStack;

import java.io.Serializable;
import java.util.Map;

/**
 * Multiplexer channel is a lightweight version of a regular channel where
 * multiple MuxChannel(s) share the same underlying regular channel.
 * 
 * <p>
 * MuxChannel has to be created with a unique application id. The multiplexer
 * keeps track of all registered applications and tags messages belonging to a
 * specific application with that id for sent messages. When receiving a message
 * from a remote peer, the multiplexer will dispatch a message to the
 * appropriate MuxChannel depending on the id attached to the message.
 * 
 * <p>
 * MuxChannel is created using {@link ChannelFactory#createMultiplexerChannel(String, String)}.
 * 
 * @author Bela Ban, Vladimir Blagojevic
 * @see ChannelFactory#createMultiplexerChannel(String, String)
 * @see JChannelFactory#createMultiplexerChannel(String, String)
 * @see Multiplexer
 * @since 2.4
 * @version $Id: MuxChannel.java,v 1.39 2007/11/15 19:40:45 vlada Exp $
 */
public class MuxChannel extends JChannel {
   
    /*
     * Header identifier
     */
    private static final String name = "MUX";

    /*
     * MuxChannel service ID
     */
    private final String id;

    /*
     * The name of the JGroups stack, e.g. as defined in stacks.xml
     */
    private final String stack_name;

    /*
     * Header added to each message sent from this MuxChannel
     */
    private final MuxHeader hdr;

    /*
     * Underlying Multiplexer  
     */
    private final Multiplexer mux;


    MuxChannel(String id, String stack_name, Multiplexer mux) {
        super(false); // don't create protocol stack, queues and threads               
        this.stack_name=stack_name;
        this.id=id;
        this.hdr=new MuxHeader(id);
        this.mux=mux;
        closed=!mux.isOpen();      
    }

    public String getStackName() {return stack_name;}

    public String getId() {return id;}

    public Multiplexer getMultiplexer() {return mux;}

    public String getChannelName() {
        return mux.getChannel().getClusterName();
    }

    public String getClusterName() {
        return mux.getChannel().getClusterName();
    }

    public Address getLocalAddress() {
        return mux != null? mux.getLocalAddress() : null;
    }

    /** This should never be used (just for testing) ! */
    public JChannel getChannel() {
        return mux.getChannel();
    }


    /**
     * Returns the <em>service</em> view, ie. the cluster view (see {@link #getView()}) <em>minus</em> the nodes on
     * which this service is not running, e.g. if S1 runs on A and C, and the cluster view is {A,B,C}, then the service
     * view is {A,C}
     * @return The service view (list of nodes on which this service is running)
     */
    public View getView() {
        return closed || !connected ? null : mux.getServiceView(id);
    }

    /** Returns the JGroups view of a cluster, e.g. if we have nodes A, B and C, then the view will
     * be {A,B,C}
     * @return The JGroups view
     */
    public View getClusterView() {
        return mux != null? mux.getChannel().getView() : null;
    }

    public ProtocolStack getProtocolStack() {
        return mux != null? mux.getChannel().getProtocolStack() : null;
    }

    public boolean isOpen() {
        return !closed;
    }

    public boolean isConnected() {
        return connected;
    }

    public Map dumpStats() {
        return mux.getChannel().dumpStats();
    }


    public void setClosed(boolean f) {
        closed=f;
    }

    public void setConnected(boolean f) {
        connected=f;
    }

    public Object getOpt(int option) {
        return mux.getChannel().getOpt(option);
    }

    public void setOpt(int option, Object value) {
        mux.getChannel().setOpt(option, value);
        super.setOpt(option, value);
    }

    public synchronized void connect(String channel_name) throws ChannelException, ChannelClosedException {
        /*make sure the channel is not closed*/
        checkClosed();

        /*if we already are connected, then ignore this*/
        if(connected) {
            if(log.isTraceEnabled()) log.trace("already connected to " + channel_name);
            return;
        }
        //add service --> MuxChannel mapping to multiplexer in case we called disconnect on this channel       
        mux.addServiceIfNotPresent(getId(), this);
        if (!mux.isConnected()) {
            mux.connect(getStackName());
        }
        try {
            if (mux.flushSupported()) {
                boolean successfulFlush = mux.startFlush(false);
                if (!successfulFlush && log.isWarnEnabled()) {
                    log.warn("Flush failed at " + mux.getLocalAddress() + ":"
                            + getId());
                }
            }
            mux.sendServiceUpMessage(getId(), mux.getLocalAddress(), true);
        } catch (Exception e) {
            if (log.isErrorEnabled())
                log.error("failed sending SERVICE_UP message", e);
        } finally {
            if (mux.flushSupported())
                mux.stopFlush();
        }
        setClosed(false);
        setConnected(true);
        notifyChannelConnected(this);
    }
 
    public synchronized void connect(String cluster_name, Address target, String state_id, long timeout) throws ChannelException {
        /*make sure the channel is not closed*/
        checkClosed();

        /*if we already are connected, then ignore this*/
        if(connected) {
            if(log.isTraceEnabled()) log.trace("already connected to " + cluster_name);
            return;
        }
        //add service --> MuxChannel mapping to multiplexer in case we called disconnect on this channel
        mux.addServiceIfNotPresent(getId(), this);
        if (!mux.isConnected()) {
            mux.connect(getStackName());
        }
        try {
            if (mux.flushSupported()) {
                boolean successfulFlush = mux.startFlush(false);
                if (!successfulFlush && log.isWarnEnabled()) {
                    log.warn("Flush failed at " + mux.getLocalAddress() + ":" + getId());
                }
            }
            try {
                mux.sendServiceUpMessage(getId(), mux.getLocalAddress(), true);
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error("failed sending SERVICE_UP message", e);
            }
            View serviceView = mux.getServiceView(getId());
            boolean stateTransferOk = false;
            boolean fetchState = serviceView != null && serviceView.size() > 1;
            if (fetchState) {
                stateTransferOk = getState(target, state_id, timeout, false);
                if (!stateTransferOk) {
                    throw new StateTransferException(
                            "Could not retrieve state " + state_id + " from "
                                    + target);
                }
            }
        } finally {
            if (mux.flushSupported())
                mux.stopFlush();
        }
        setClosed(false);
        setConnected(true);
        notifyChannelConnected(this);                       
    }

    public synchronized void disconnect() {
        if (!connected)
            return;

        setClosed(false);
        setConnected(false);
        
        try {                       
            if (mux.flushSupported()) {
                boolean successfulFlush = mux.startFlush(false);
                if (!successfulFlush && log.isWarnEnabled()) {
                    log.warn("Flush failed at " + mux.getLocalAddress() +":"+ getId());
                }
            }
            try {    
                mux.sendServiceDownMessage(getId(), mux.getLocalAddress(),true);
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error("failed sending SERVICE_DOWN message", e);
            }            
        } catch (Throwable t) {
            log.error("disconnecting channel failed", t);
        }
        finally {
            if (mux.flushSupported())
                mux.stopFlush();                      
        }    
        // disconnects JChannel if all MuxChannels are
        // in disconnected state
        mux.disconnect();
        notifyChannelDisconnected(this);
    }



    public synchronized void open() throws ChannelException {
        
        if (!mux.isOpen())
                mux.open();  
        
        setClosed(false);
        setConnected(false); // needs to be connected next        
    }

    public synchronized void close() {
        if(closed)
            return;
        
        setClosed(true);
        setConnected(false);
        
        try {                        
            if (mux.flushSupported()) {
                boolean successfulFlush = mux.startFlush(false);
                if (!successfulFlush && log.isWarnEnabled()) {
                    log.warn("Flush failed at " + mux.getLocalAddress() + ":" + getId());
                }
            }
            try {    
                mux.sendServiceDownMessage(getId(), mux.getLocalAddress(), true);
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error("failed sending SERVICE_DOWN message", e);
            }             
        }
        catch (Throwable t) {
            log.error("closing channel failed", t);
        }
        finally {            
            if (mux.flushSupported())
                mux.stopFlush();                     
        }            
        closeMessageQueue(true);
        notifyChannelClosed(this);
    }

    protected void _close(boolean disconnect, boolean close_mq) {
        super._close(disconnect, close_mq);
        closed=!mux.isOpen();
        setConnected(mux.isConnected());
        notifyChannelClosed(this);
    }

    public synchronized void shutdown() {
        if(closed)
            return;
        
        setClosed(true);
        setConnected(false);
        
        try {            
            if (mux.flushSupported()) {
                boolean successfulFlush = mux.startFlush(false);
                if (!successfulFlush && log.isWarnEnabled()) {
                    log.warn("Flush failed at " + mux.getLocalAddress()
                            + ":" + getId());
                }
            }
            try {    
                mux.sendServiceDownMessage(getId(), mux.getLocalAddress(),true);
            } catch (Exception e) {
                if (log.isErrorEnabled())
                    log.error("failed sending SERVICE_DOWN message", e);
            }
        } 
        catch (Throwable t) {
            log.error("shutdown channel failed", t);
        }
        finally {
            if (mux.flushSupported())
                mux.stopFlush();            
        }                
        closeMessageQueue(true);
        notifyChannelClosed(this);
    }


    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        msg.putHeader(name, hdr);
        mux.getChannel().send(msg);
    }

    public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException {
        send(new Message(dst, src, obj));
    }


    public void down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            msg.putHeader(name, hdr);            
        }
        mux.getChannel().down(evt);
    }

    public Object downcall(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            msg.putHeader(name, hdr);
        }
        return mux.getChannel().downcall(evt);
    }
    

    public boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return getState(target, null, timeout);
    }

    public boolean getState(Address target, String state_id, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return getState(target,state_id,timeout,true);
    }
    
    public boolean getState(Address target, String state_id, long timeout,boolean useFlushIfPresent) throws ChannelNotConnectedException, ChannelClosedException {
        String my_id=id;

        if(state_id != null)
            my_id += "::" + state_id;

        // we're usig service views, so we need to find the first host in the cluster on which our service runs
        // http://jira.jboss.com/jira/browse/JGRP-247
        //
        // unless service runs on a specified target node
        // http://jira.jboss.com/jira/browse/JGRP-401
        Address service_view_coordinator=mux.getStateProvider(target,id);
        Address tmp=getLocalAddress();

        if(service_view_coordinator != null)
            target=service_view_coordinator;

        if(tmp != null && tmp.equals(target)) // this will avoid the "cannot get state from myself" error
            target=null;

        if(!mux.stateTransferListenersPresent())
            return mux.getChannel().getState(target, my_id, timeout, useFlushIfPresent);
        else {
            return mux.getState(target, my_id, timeout);
        }
    }

    public void returnState(byte[] state) {
        mux.getChannel().returnState(state, id);
    }

    public void returnState(byte[] state, String state_id) {
        String my_id=id;
        if(state_id != null)
            my_id+="::" + state_id;
        mux.getChannel().returnState(state, my_id);
    }  
}
