package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.util.*;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * TCP connection which (despite the fancy name) blocks only a single thread at most. Uses a bounded queue, to which
 * senders add their messages, and a single consumer sending the messages. When the queue is full, messages will
 * get dropped. Therefore, at most one thread is blocked on TCP write when the send-window is full.<br/>
 * Link: https://issues.redhat.com/browse/JGRP-2759
 * @author Bela Ban
 * @since  5.3.3
 */
public class TcpConnectionNonBlocking extends TcpConnection implements Runnable {
    protected final BlockingQueue<ByteArray> queue;
    protected int                            max_size=1024;
    protected final Runner                   sender;
    protected final LongAdder                dropped_msgs=new LongAdder();
    protected final FastArray<ByteArray>     remove_queue;
    protected int                            rq_capacity;  // by default 20% of max_size


    public TcpConnectionNonBlocking(Address peer_addr, TcpBaseServer server, int max_size) throws Exception {
        super(peer_addr, server);
        this.max_size=max_size;
        queue=new ConcurrentLinkedBlockingQueue<>(max_size, true, false);
        remove_queue=new FastArray<>(rq_capacity=max_size/5);
        sender=new Runner(server.factory, String.format("sender to %s", peer_addr), this, null);
    }

    public TcpConnectionNonBlocking(Socket s, TcpServer server, int max_size) throws Exception {
        super(s, server);
        this.max_size=max_size;
        queue=new ConcurrentLinkedBlockingQueue<>(max_size, true, false);
        remove_queue=new FastArray<>(rq_capacity=max_size/5);
        sender=new Runner(server.factory, String.format("sender to %s", peer_addr), this, null);
    }

    public int     maxSize()         {return max_size;}
    public long    droppedMessages() {return dropped_msgs.sum();}
    public int     queueSize()       {return queue != null? queue.size() : 0;}
    public boolean senderRunning()   {return sender.isRunning();}

    @Override public void start() {
        super.start();
        sender.start();
    }

    @Override public void close() throws IOException {
        super.close();
        sender.stop();
    }

    @Override
    public void send(byte[] data, int offset, int length) throws Exception {
        // to be on the safe side, we copy the data: some bundlers (e.g. TransferQueueBundler) reuse a buffer to
        // serialize messages to and - before the data is sent by the sender thread - the buffer might be changed!
        // This is similar to what NioConnection does on a partial write. If the send was synchronous (like in
        // TcpConnection), we would not have to copy the data
        ByteArray buf=new ByteArray(data, offset, length).copy();
        boolean added=queue.offer(buf);
        if(!added)
            dropped_msgs.increment();
    }

    public void run() {
        try {
            if(!isConnected())
                return;
            do {
                remove_queue.clear(false);
                remove_queue.add(queue.take());
                queue.drainTo(remove_queue, rq_capacity - 1);
                for(ByteArray data: remove_queue) {
                    // no synchronization needed as this thread is the only sender
                    doSend(data.getArray(), data.getOffset(), data.getLength(), false);
                }
                flush();
            }
            while(!queue.isEmpty());
        }
        catch(InterruptedException iex) {
            ;
        }
        catch(EOFException | SocketException ex) {
            ; // regular use case when a peer closes its connection - we don't want to log this as exception
        }
        catch(Exception e) {
            if(e instanceof SSLException && e.getMessage().contains("Socket closed")) {
                ; // regular use case when a peer closes its connection - we don't want to log this as exception
            }
            else if(e instanceof SSLHandshakeException && e.getCause() instanceof EOFException) {
                ; // Ignore SSL handshakes closed early (usually liveness probes)
            }
            else {
                if(server.logDetails())
                    server.log.warn("failed sending message", e);
                else
                    server.log.warn("failed sending message: " + e);
                server.notifyConnectionClosed(TcpConnectionNonBlocking.this);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s [%d/%d, %d drops, sender: %b]", super.toString(), queueSize(), maxSize(),
                             droppedMessages(), senderRunning());
    }

    protected String name() {
        InetAddress local=sock.getLocalAddress(), remote=sock.getInetAddress();
        String l=local != null? Util.shortName(local) : "<null>";
        String r=remote != null? Util.shortName(remote) : "<null>";
        return String.format("Connection.Sender [%s:%s-%s:%s]", l, sock.getLocalPort(), r, sock.getPort());
    }

}
