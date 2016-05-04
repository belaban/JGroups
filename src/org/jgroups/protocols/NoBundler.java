package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Bundler which doesn't bundle :-) Can be used to measure the diff between bundling and non-bundling (e.g. at runtime)
 * @author Bela Ban
 * @since  3.6.10
 */
public class NoBundler implements Bundler {
    protected TP                              transport;
    protected final ReentrantLock             lock=new ReentrantLock();
    protected Log                             log;
    protected final ByteArrayDataOutputStream output=new ByteArrayDataOutputStream(1024);


    public void init(TP transport) {
        this.transport=transport;
        log=transport.getLog();
    }
    public void start() {}
    public void stop()  {}

    public void send(Message msg) throws Exception {
        lock.lock();
        try {
            sendSingleMessage(msg, output != null? output : new ByteArrayDataOutputStream((int)(msg.size() + MSG_OVERHEAD)));
        }
        finally {
            lock.unlock();
        }
    }


    protected void sendSingleMessage(final Message msg, final ByteArrayDataOutputStream output) {
        Address dest=msg.getDest();
        try {
            output.position(0);
            Util.writeMessage(msg, output, dest == null);
            transport.doSend(transport.getClusterName(msg), output.buffer(), 0, output.position(), dest);
            if(transport.statsEnabled())
                transport.incrSingleMsgsInsteadOfBatches();
        }
        catch(SocketException sock_ex) {
            log.trace(Util.getMessage("SendFailure"),
                      transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), sock_ex.toString(), msg.printHeaders());
        }
        catch(Throwable e) {
            log.error(Util.getMessage("SendFailure"),
                      transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
    }


    protected static class Output {
        protected final AtomicBoolean             available=new AtomicBoolean(true);
        protected final ByteArrayDataOutputStream output=new ByteArrayDataOutputStream(1024);
    }

}