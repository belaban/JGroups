package org.jgroups.mux;

import org.jgroups.*;

import java.io.Serializable;
import java.util.Map;
import java.util.Vector;

/**
 * Multiplexer channel. This is returned as result of calling
 * {@link org.jgroups.ChannelFactory#createMultiplexerChannel(String, String)}. Maintains the multiplexer
 * ID, which is used to add a header to each message, so that the message can be demultiplexed at the receiver
 * @author Bela Ban
 * @version $Id: MuxChannel.java,v 1.5 2006/03/14 09:08:21 belaban Exp $
 */
public class MuxChannel extends JChannel {

    /** the real channel to delegate to */
    final JChannel ch;

    /** The application ID */
    final String id;

    /** a reference back to the factory that created us */
    final JChannelFactory factory;

    /** The name of the JGroups stack, e.g. as defined in stacks.xml */
    final String stack_name;

    /** will be added to each message sent */
    final MuxHeader hdr;

    final String name="MUX";



    public MuxChannel(JChannelFactory f, JChannel ch, String id, String stack_name) throws ChannelException {
        super(false); // don't create protocol stack, queues and threads
        factory=f;
        this.ch=ch;
        this.stack_name=stack_name;
        this.id=id;
        hdr=new MuxHeader(id);
        closed=!ch.isOpen();
        connected=ch.isConnected();
    }

    public String getStackName() {return stack_name;}

    public String getId() {return id;}

    public String getChannelName() {
        return ch.getChannelName();
    }

    public Address getLocalAddress() {
        return ch != null? ch.getLocalAddress() : null;
    }

    public View getView() {
        return ch != null? ch.getView() : null;
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

    public synchronized void connect(String channel_name) throws ChannelException, ChannelClosedException {
        ch.connect(stack_name);
        closed=!ch.isOpen();
        connected=ch.isConnected();
        notifyChannelConnected(this);
    }

    public synchronized void disconnect() {
        factory.disconnect(this);
        notifyChannelDisconnected(this);
    }



    public synchronized void open() throws ChannelException {
        ch.open();
        closed=!ch.isOpen();
        connected=ch.isConnected();
    }

    public synchronized void close() {
        factory.close(this);
        // closeMessageQueue(true);
    }

    protected void _close(boolean disconnect, boolean close_mq) {
        super._close(disconnect, close_mq);
        closed=!ch.isOpen();
        connected=ch.isConnected();
        notifyChannelClosed(this);
    }

    public synchronized void shutdown() {

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
    }


    public void blockOk() {
    }

    public boolean getState(Address target, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return false;
    }

    public boolean getAllStates(Vector targets, long timeout) throws ChannelNotConnectedException, ChannelClosedException {
        return false;
    }

    public void returnState(byte[] state) {
    }

    public void suspend() {
        ch.suspend();
    }

    public void resume() {
        ch.resume();
    }

    public boolean isSuspended() {
        return ch.isSuspended();
    }


    protected void checkNotConnected() throws ChannelNotConnectedException {
        ;
    }

    protected void checkClosed() throws ChannelClosedException {
        ;
    }
}
