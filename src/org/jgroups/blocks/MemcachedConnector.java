package org.jgroups.blocks;

import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

/** Class which listens on a server socket for memcached clients, reads the requests, forwards them to an instance of
 * PartitionedHashMap and sends the response. A memcached client should be able to work without changes once the
 * memcached protocol (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt) has been implemented
 * completely.
 * @author Bela Ban
 * @version $Id: MemcachedConnector.java,v 1.2 2008/08/26 14:47:05 belaban Exp $
 */
public class MemcachedConnector implements Runnable {
    private int port=11211;
    private InetAddress bind_addr=null;
    private PartitionedHashMap cache=null;
    private Thread thread=null;
    private ServerSocketChannel srv_sock_channel;
    private ServerSocket srv_sock;
    private Selector selector;

    private int core_threads=1, max_threads=100;
    private long idle_time=5000L;
    private ExecutorService thread_pool;



    public MemcachedConnector(InetAddress bind_addr, int port, PartitionedHashMap cache) {
        this.bind_addr=bind_addr;
        this.cache=cache;
        this.port=port;
    }


    public InetAddress getBindAddress() {
        return bind_addr;
    }

    public void setBindAddress(InetAddress bind_addr) {
        this.bind_addr=bind_addr;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port=port;
    }

    public PartitionedHashMap getCache() {
        return cache;
    }

    public void setCache(PartitionedHashMap cache) {
        this.cache=cache;
    }

    public int getThreadPoolCoreThreads() {
        return core_threads;
    }

    public void setThreadPoolCoreThreads(int core_threads) {
        this.core_threads=core_threads;
    }

    public int getThreadPoolMaxThreads() {
        return max_threads;
    }

    public void setThreadPoolMaxThreads(int max_threads) {
        this.max_threads=max_threads;
    }

    public long getThreadPoolIdleTime() {
        return idle_time;
    }

    public void setThreadPoolIdleTime(long idle_time) {
        this.idle_time=idle_time;
    }

    public ExecutorService getThreadPool() {
        return thread_pool;
    }

    public void setThreadPool(ExecutorService thread_pool) {
        this.thread_pool=thread_pool;
    }

    
    public void start() throws IOException {
        srv_sock_channel=ServerSocketChannel.open();
        srv_sock_channel.configureBlocking(false);
        srv_sock=srv_sock_channel.socket();
        srv_sock.bind(new InetSocketAddress(bind_addr, port));

        thread_pool=new ThreadPoolExecutor(core_threads, max_threads, idle_time, TimeUnit.MILLISECONDS,
                                           new LinkedBlockingQueue<Runnable>(100), new ThreadPoolExecutor.CallerRunsPolicy());

        selector=Selector.open();
        srv_sock_channel.register(selector, SelectionKey.OP_ACCEPT);
        if(thread == null || !thread.isAlive()) {
            thread=new Thread(this, "Acceptor");
            thread.start();
        }
    }

    public void stop() throws IOException {
        Util.close(srv_sock);
        srv_sock_channel.close();
        thread=null;
        thread_pool.shutdown();
    }

    public void run() {
        System.out.println("MemcachedConnector listening on " + srv_sock.getLocalSocketAddress());
        while(thread != null && Thread.currentThread().equals(thread)) {
            try {
                if(selector.select() > 0) {
                    Set<SelectionKey> keys=selector.selectedKeys();
                    for(Iterator<SelectionKey> it=keys.iterator(); it.hasNext();) {
                        SelectionKey key=it.next();
                        it.remove();
                        SocketChannel client_channel;
                        if(key.isAcceptable()) {
                            ServerSocketChannel tmp=(ServerSocketChannel)key.channel();
                            client_channel=tmp.accept();
                            client_channel.configureBlocking(false);
                            client_channel.register(selector, SelectionKey.OP_READ);
                            // handleConnection(client_channel);
                        }
                        else if(key.isReadable()) {
                            client_channel=(SocketChannel)key.channel();
                            handleRead(client_channel);
                        }
                    }
                }
            }
            catch(ClosedSelectorException closed) {
                break;
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRead(final SocketChannel client_channel) throws IOException {
        Socket client_sock=client_channel.socket();
        System.out.println("got data from " + client_sock.getInetAddress() + ":" + client_sock.getPort());
        ByteBuffer buf=ByteBuffer.wrap("hello from bela\r\n".getBytes());
        client_channel.write(buf);
        client_channel.close();
    }
}

