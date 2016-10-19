package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;

/**
 * Bundler which doesn't bundle :-) Can be used to measure the diff between bundling and non-bundling (e.g. at runtime)
 * @author Bela Ban
 * @since  4.0
 */
public class NoBundler implements Bundler {
    protected TP                                       transport;
    protected Log                                      log;
    protected int                                      pool_size=10;
    protected BlockingQueue<ByteArrayDataOutputStream> buf_pool;
    protected int                                      initial_buf_size=512;

    // protected final Profiler                           send=new Profiler("nb.send", TimeUnit.MICROSECONDS);


    public int       size()                {return 0;}
    public int       initialBufSize()      {return initial_buf_size;}
    public NoBundler initialBufSize(int s) {this.initial_buf_size=s; return this;}
    public int       poolSize()            {return pool_size;}

    public NoBundler poolSize(int s) {
        if(s == pool_size) return this;
        pool_size=s;
        BlockingQueue<ByteArrayDataOutputStream> new_pool=new ArrayBlockingQueue<>(pool_size);
        BlockingQueue<ByteArrayDataOutputStream> tmp=buf_pool;
        buf_pool=new_pool;
        if(tmp != null)
            tmp.clear();
        return this;
    }

    public void init(TP transport) {
        this.transport=transport;
        log=transport.getLog();
        buf_pool=new ArrayBlockingQueue<>(pool_size);
        IntStream.rangeClosed(1, pool_size).forEach(ignored -> buf_pool.offer(new ByteArrayDataOutputStream(initial_buf_size)));
        // transport.registerProbeHandler(send);
    }
    public void start() {}
    public void stop()  {}

    public void send(Message msg) throws Exception {
        ByteArrayDataOutputStream out=null;
        try {
            out=buf_pool.poll();
            if(out == null) {
                out=new ByteArrayDataOutputStream(initial_buf_size);
                log.warn("created new output buffer as pool was empty");
            }
            sendSingleMessage(msg, out);
        }
        finally {
            if(out != null)
                buf_pool.offer(out);
        }
    }


    protected void sendSingleMessage(final Message msg, final ByteArrayDataOutputStream output) {
        Address dest=msg.getDest();
        try {
            output.position(0);
            Util.writeMessage(msg, output, dest == null);
            transport.doSend(output.buffer(), 0, output.position(), dest);
            if(transport.statsEnabled())
                transport.num_single_msgs_sent++;
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

}