package org.jgroups.blocks;

import org.jgroups.util.Buffer;
import org.jgroups.util.DirectExecutor;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/** Class which listens on a server socket for memcached clients, reads the requests, forwards them to an instance of
 * PartitionedHashMap and sends the response. A memcached client should be able to work without changes once the
 * memcached protocol (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt) has been implemented
 * completely.<br/>
 * toto list:
 * <ul>
 * <li>Currently, we use a static buffer of 4095 bytes. Requests with bigger payloads (= values) will fail. Allocate
 *     a byte buffer based on the number of bytes sent in the request (e.g. set())
 * </ul>
 * @author Bela Ban
 * @version $Id: MemcachedConnector.java,v 1.5 2008/08/27 12:12:26 belaban Exp $
 */
public class MemcachedConnector implements Runnable {
    private int port=11211;
    private InetAddress bind_addr=null;
    private PartitionedHashMap<String, Buffer> cache=null;
    private Thread thread=null;
    private ServerSocketChannel srv_sock_channel;
    private ServerSocket srv_sock;
    private Selector selector;

    private int core_threads=1, max_threads=100;
    private long idle_time=5000L;
    private Executor thread_pool;



    public MemcachedConnector(InetAddress bind_addr, int port, PartitionedHashMap<String, Buffer> cache) {
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

    public PartitionedHashMap<String, Buffer> getCache() {
        return cache;
    }

    public void setCache(PartitionedHashMap<String, Buffer> cache) {
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

    public Executor getThreadPool() {
        return thread_pool;
    }

    public void setThreadPool(Executor thread_pool) {
        if(this.thread_pool instanceof ExecutorService) {
            ((ExecutorService)thread_pool).shutdown();
        }
        this.thread_pool=thread_pool;
    }



    
    public void start() throws IOException {
        srv_sock_channel=ServerSocketChannel.open();
        srv_sock_channel.configureBlocking(false);
        srv_sock=srv_sock_channel.socket();
        srv_sock.bind(new InetSocketAddress(bind_addr, port));

        if(thread_pool == null) {
            // thread_pool=new ThreadPoolExecutor(core_threads, max_threads, idle_time, TimeUnit.MILLISECONDS,
            //                               new LinkedBlockingQueue<Runnable>(100), new ThreadPoolExecutor.CallerRunsPolicy());
            thread_pool=new DirectExecutor();
        }

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
        if(thread_pool instanceof ExecutorService)
            ((ExecutorService)thread_pool).shutdown();
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
                        if(!key.isValid())
                            continue;
                        if(key.isAcceptable()) {
                            ServerSocketChannel tmp=(ServerSocketChannel)key.channel();
                            client_channel=tmp.accept();
                            client_channel.configureBlocking(false);
                            client_channel.register(selector, SelectionKey.OP_READ);
                        }
                        else if(key.isReadable()) {
                            client_channel=(SocketChannel)key.channel();
                            client_channel.configureBlocking(true);
                            RequestHandler handler=new RequestHandler(cache, client_channel);
                            thread_pool.execute(handler);
                            client_channel.configureBlocking(false);
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


    private static class RequestHandler implements Runnable {
        private final PartitionedHashMap<String,Buffer> cache;
        private final SocketChannel client_channel;
        private static final ByteBuffer STORED=ByteBuffer.wrap("STORED\r\n".getBytes());
        private static final ByteBuffer END=ByteBuffer.wrap("END\r\n".getBytes());

        public RequestHandler(PartitionedHashMap<String,Buffer> cache, SocketChannel client_channel) {
            this.cache=cache;
            this.client_channel=client_channel;
        }

        public void run() {
            Socket client_sock=client_channel.socket();

            ByteBuffer buf=ByteBuffer.allocate(4095);
            int num;

            try {
                num=client_channel.read(buf);
                if(num > 0)
                    ;
                else if(num == -1) {
                    client_channel.close();
                    Util.close(client_sock); // needed ?
                    return;
                }
                buf.flip();
                Request request=parseRequest(buf);
                if(request == null) {
                    // send error message to client
                    return;
                }
                switch(request.type) {
                    case SET:
                        if(request.number_of_bytes > buf.limit()) {
                            // todo: allocate a bigger buffer, copy part of the old buffer into the new one and
                            // then read the remaining bytes from the socket channel
                        }
                        Buffer tmp=new Buffer(buf.array(), buf.position(), buf.remaining() -2); // minus the trailing /r/n
                        cache.put(request.key, tmp, request.caching_time);
                        client_channel.write(STORED);
                        break;

                    case GET:
                    case GETS:
                        if(request.get_key_list != null && !request.get_key_list.isEmpty()) {
                            for(String key: request.get_key_list) {
                                Buffer val=cache.get(key);
                                if(val != null) {
                                    int length=val.getLength();
                                    ByteBuffer result=ByteBuffer.allocate(length + 255);
                                    Util.writeString(result, "VALUE 0 " + length + "\r\n");
                                    result.put(val.getBuf(), val.getOffset(), val.getLength());
                                    Util.writeString(result, "\r\n");
                                    result.flip();
                                    client_channel.write(result);
                                }
                            }
                        }
                        client_channel.write(END);
                        break;
                }
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }

        private static Request parseRequest(ByteBuffer buf) {
            Request req=new Request();
            String tmp=Util.parseString(buf);
            if(tmp.equals("set"))
                req.type=Request.Type.SET;
            else if(tmp.equals("add"))
                req.type=Request.Type.ADD;
            else if(tmp.equals("replace"))
                req.type=Request.Type.REPLACE;
            else if(tmp.equals("prepend"))
                req.type=Request.Type.PREPEND;
            else if(tmp.equals("append"))
                req.type=Request.Type.APPEND;
            else if(tmp.equals("cas"))
                req.type=Request.Type.CAS;
            else if(tmp.equals("incr"))
                req.type=Request.Type.INCR;
            else if(tmp.equals("decr"))
                req.type=Request.Type.DECR;
            else if(tmp.equals("get"))
                req.type=Request.Type.GET;
            else if(tmp.equals("gets"))
                req.type=Request.Type.GETS;
            else if(tmp.equals("delete"))
                req.type=Request.Type.DELETE;
            else if(tmp.equals("stat"))
                req.type=Request.Type.STAT;
            else if(tmp.equals("stats"))
                req.type=Request.Type.STATS;

            switch(req.type) {
                case SET:
                case ADD:
                case REPLACE:
                case PREPEND:
                case APPEND:

                    // key
                    tmp=Util.parseString(buf);
                    req.key=tmp;

                    // read flags and discard: flags are not supported
                    tmp=Util.parseString(buf);

                    // expiry time
                    tmp=Util.parseString(buf);
                    req.caching_time=Long.parseLong(tmp) * 1000L; // convert from secs to ms

                    // number of bytes
                    tmp=Util.parseString(buf, false);
                    req.number_of_bytes=Integer.parseInt(tmp);

                    Util.readNewLine(buf);
                    break;
                case GET:
                case GETS:
                    req.get_key_list=new ArrayList<String>(5);
                    while(true) {
                        // key(s)
                        tmp=Util.parseString(buf);
                        if(tmp == null || tmp.length() == 0)
                            break;
                        req.get_key_list.add(tmp);
                    }
                    break;
            }

            return req;
        }

    }


    public static class Request {
        public static enum Type {SET, ADD, REPLACE, PREPEND, APPEND, CAS, INCR, DECR, GET, GETS, DELETE, STAT, STATS};

        Type type;
        String key;
        List<String> get_key_list=null;
        long caching_time;
        int number_of_bytes=0;

        public Request() {
        }

        private Request(Type type) {
            this.type=type;
        }

        private Request(Type type, String key) {
            this(type);
            this.key=key;
        }

        public String toString() {
            return type + ": key=" + key + ", key_list=" + get_key_list +
                    ", caching_time=" + caching_time + ", number_of_bytes=" + number_of_bytes;
        }
    }
}

