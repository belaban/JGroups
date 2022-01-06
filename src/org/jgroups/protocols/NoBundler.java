package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.util.Objects;

/**
 * Bundler which doesn't bundle :-) Can be used to measure the diff between bundling and non-bundling (e.g. at runtime)
 * This bundler doesn't use a pool of buffers, but creates a new buffer every time a message is sent.
 * @author Bela Ban
 * @since  4.0
 */
public class NoBundler implements Bundler {
    protected TP                                       transport;
    protected Log                                      log;

    public int size()       {return 0;}
    public int getMaxSize() {return 64000;}

    public void init(TP transport) {
        this.transport=Objects.requireNonNull(transport);
        log=transport.getLog();
    }

    public int getQueueSize() {
        return -1;
    }

    public void start() {}
    public void stop()  {}

    public void send(Message msg) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(msg.size() + 10);
        sendSingleMessage(msg, out);
    }


    protected void sendSingleMessage(final Message msg, final ByteArrayDataOutputStream output) throws Exception {
        Address dest=msg.getDest();
        output.position(0);
        Util.writeMessage(msg, output, dest == null);
        transport.doSend(output.buffer(), 0, output.position(), dest);
        if(transport.statsEnabled())
            transport.getMessageStats().incrNumSingleMsgsSent(1);
    }

}