package org.jgroups.mux;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;

import java.io.Serializable;
import java.util.Map;
import java.util.Vector;

/**
 * Multiplexer channel. This is returned as result of calling
 * {@link org.jgroups.ChannelFactory#createMultiplexerChannel(String, String)}. Maintains the multiplexer
 * ID, which is used to add a header to each message, so that the message can be demultiplexed at the receiver
 * @author Bela Ban
 * @version $Id: MuxChannel.java,v 1.3 2006/03/12 11:49:27 belaban Exp $
 */
public class MuxChannel extends Channel {

    /** the real channel to delegate to */
    final JChannel ch;

    /** a reference back to the factory that created us */
    final JChannelFactory factory;

    /** The name of the JGroups stack, e.g. as defined in stacks.xml */
    final String stack_name;

    /** will be added to each message sent */
    final MuxHeader hdr;

    final String name="MUX";


    protected final Log log=LogFactory.getLog(getClass());


    public MuxChannel(JChannelFactory f, JChannel ch, String id, String stack_name) {
        factory=f;
        this.ch=ch;
        this.stack_name=stack_name;
        hdr=new MuxHeader(id);
    }


    protected Log getLog() {
        return log;
    }

    public void connect(String channel_name) throws ChannelException, ChannelClosedException {
        ch.connect(stack_name);
    }

    public void disconnect() {
    }

    public void close() {
    }

    public synchronized void shutdown() {

    }

    public boolean isOpen() {
        return false;
    }

    public boolean isConnected() {
        return false;
    }

    public Map dumpStats() {
        return null;
    }

    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException {
        msg.putHeader(name, hdr);
        ch.send(msg);
    }

    public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException {
    }


    public void down(Event evt) {
        super.down(evt);
    }

    public Object receive(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {
        return null;
    }

    public void up(Event evt) {
        System.out.println("received " + evt);
    }

    public Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException, TimeoutException {
        return null;
    }

    public View getView() {
        return null;
    }

    public Address getLocalAddress() {
        return null;
    }

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
