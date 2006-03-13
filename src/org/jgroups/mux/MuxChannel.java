package org.jgroups.mux;

import org.apache.commons.logging.Log;
import org.jgroups.*;

import java.io.Serializable;
import java.util.Map;
import java.util.Vector;

/**
 * Multiplexer channel. This is returned as result of calling
 * {@link org.jgroups.ChannelFactory#createMultiplexerChannel(String, String)}. Maintains the multiplexer
 * ID, which is used to add a header to each message, so that the message can be demultiplexed at the receiver
 * @author Bela Ban
 * @version $Id: MuxChannel.java,v 1.4 2006/03/13 09:24:30 belaban Exp $
 */
public class MuxChannel extends JChannel {

    /** the real channel to delegate to */
    final JChannel ch;

    /** a reference back to the factory that created us */
    final JChannelFactory factory;

    /** The name of the JGroups stack, e.g. as defined in stacks.xml */
    final String stack_name;

    /** will be added to each message sent */
    final MuxHeader hdr;

    final String name="MUX";



    public MuxChannel(JChannelFactory f, JChannel ch, String id, String stack_name) throws ChannelException {
        factory=f;
        this.ch=ch;
        this.stack_name=stack_name;
        hdr=new MuxHeader(id);
    }

    public Address getLocalAddress() {
        return ch != null? ch.getLocalAddress() : null;
    }

    protected Log getLog() {
        return log;
    }

    public synchronized void connect(String channel_name) throws ChannelException, ChannelClosedException {
        ch.connect(stack_name);
    }

    public synchronized void disconnect() {
    }

    public synchronized void close() {
    }

    public synchronized void shutdown() {

    }

    public boolean isOpen() {
        return ch.isOpen();
    }

    public boolean isConnected() {
        return ch.isConnected();
    }

    protected void checkNotConnected() throws ChannelNotConnectedException {
        ;
    }

    protected void checkClosed() throws ChannelClosedException {
        ;
    }

    public Map dumpStats() {
        return null;
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

//    public Object receive(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {
//        return null;
//    }

    public Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {
        return null;
    }

//    public View getView() {
//        return null;
//    }

//    public Address getLocalAddress() {
//        return null;
//    }

    public String getChannelName() {
        return null;
    }

    public void setOpt(int option, Object value) {
    }

    public Object getOpt(int option) {
        return null;
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
}
