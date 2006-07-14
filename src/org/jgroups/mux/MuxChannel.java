package org.jgroups.mux;

import org.jgroups.*;
import org.jgroups.stack.ProtocolStack;

import java.io.Serializable;
import java.util.Map;

/**
 * Multiplexer channel. This is returned as result of calling
 * {@link org.jgroups.ChannelFactory#createMultiplexerChannel(String,String,boolean,String)}. Maintains the multiplexer
 * ID, which is used to add a header to each message, so that the message can be demultiplexed at the receiver
 * @author Bela Ban
 * @version $Id: MuxChannel.java,v 1.18 2006/07/14 12:17:24 belaban Exp $
 */
public class MuxChannel extends JChannel {

    /** the real channel to delegate to */
    final JChannel ch;

    /** The service ID */
    final String id;

    /** a reference back to the factory that created us */
    final JChannelFactory factory;

    /** The name of the JGroups stack, e.g. as defined in stacks.xml */
    final String stack_name;

    /** will be added to each message sent */
    final MuxHeader hdr;

    final String name="MUX";
    final Multiplexer mux;


    public MuxChannel(JChannelFactory f, JChannel ch, String id, String stack_name, Multiplexer mux) {
        super(false); // don't create protocol stack, queues and threads
        factory=f;
        this.ch=ch;
        this.stack_name=stack_name;
        this.id=id;
        hdr=new MuxHeader(id);
        this.mux=mux;
        closed=!ch.isOpen();
        connected=ch.isConnected();
    }

    public String getStackName() {return stack_name;}

    public String getId() {return id;}

    public Multiplexer getMultiplexer() {return mux;}

    public String getChannelName() {
        return ch.getChannelName();
    }

    public Address getLocalAddress() {
        return ch != null? ch.getLocalAddress() : null;
    }

    /**
     * Returns the <em>service</em> view, ie. the cluster view (see {@link #getView()}) <em>minus</em> the nodes on
     * which this service is not running, e.g. if S1 runs on A and C, and the cluster view is {A,B,C}, then the service
     * view is {A,C}
     * @return The service view (list of nodes on which this service is running)
     */
    public View getView() {
        return mux.getServiceView(id);
    }

    /** Returns the JGroups view of a cluster, e.g. if we have nodes A, B and C, then the view will
     * be {A,B,C}
     * @return The JGroups view
     */
    public View getClusterView() {
        return ch != null? ch.getView() : null;
    }

    public ProtocolStack getProtocolStack() {
        return ch != null? ch.getProtocolStack() : null;
    }

    public boolean isOpen() {
        return !closed;
    }

    public boolean isConnected() {
        return connected;
    }

    public Map dumpStats() {
        return ch.dumpStats();
    }


    public void setClosed(boolean f) {
        closed=f;
    }

    public void setConnected(boolean f) {
        connected=f;
    }

    public Object getOpt(int option) {
        return ch.getOpt(option);
    }

    public void setOpt(int option, Object value) {
        ch.setOpt(option, value);
        super.setOpt(option, value);
    }

    public synchronized void connect(String channel_name) throws ChannelException, ChannelClosedException {
        factory.connect(this);
        notifyChannelConnected(this);
    }

    public synchronized void disconnect() {
        try {
            factory.disconnect(this);
        }
        finally {
            closed=false;
            connected=false;
        }
        notifyChannelDisconnected(this);
    }



    public synchronized void open() throws ChannelException {
        factory.open(this);
    }

    public synchronized void close() {
        try {
            factory.close(this);
        }
        finally {
            closed=true;
            connected=false;
            closeMessageQueue(true);
        }
        notifyChannelClosed(this);
    }

    protected void _close(boolean disconnect, boolean close_mq) {
        super._close(disconnect, close_mq);
        closed=!ch.isOpen();
        connected=ch.isConnected();
        notifyChannelClosed(this);
    }

    public synchronized void shutdown() {
        try {
            factory.shutdown(this);
        }
        finally {
            closed=true;
            connected=false;
            closeMessageQueue(true);
        }
    }


    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        msg.putHeader(name, hdr);
        ch.send(msg);
    }

    public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException {
        send(new Message(dst, src, obj));
    }

    public void down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            msg.putHeader(name, hdr);
            ch.down(evt);
        }
        else
            ch.down(evt);
    }

    public void blockOk() {
    }


    public boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return getState(target, null, timeout);
    }

    public boolean getState(Address target, String state_id, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        String my_id=id;

        if(state_id != null)
            my_id += "::" + state_id;

        if(!mux.stateTransferListenersPresent())
            return ch.getState(target, my_id, timeout);
        else {
            return mux.getState(target, my_id, timeout);
        }
    }

    public void returnState(byte[] state) {
        ch.returnState(state, id);
    }

    public void returnState(byte[] state, String state_id) {
        String my_id=id;
        if(state_id != null)
            my_id+="::" + state_id;
        ch.returnState(state, my_id);
    }


    protected void checkNotConnected() throws ChannelNotConnectedException {
        ;
    }

    protected void checkClosed() throws ChannelClosedException {
        ;
    }
}
