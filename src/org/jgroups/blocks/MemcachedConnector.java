package org.jgroups.blocks;

import org.jgroups.util.Buffer;
import org.jgroups.util.DirectExecutor;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/** Class which listens on a server socket for memcached clients, reads the requests, forwards them to an instance of
 * PartitionedHashMap and sends the response. A memcached client should be able to work without changes once the
 * memcached protocol (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt) has been implemented
 * completely.<br/>
 * toto list:
 * <ul>
 * <li>Expose JMX stats and register with MBeanServer
 * </ul>
 * @author Bela Ban
 * @version $Id: MemcachedConnector.java,v 1.9 2008/08/27 15:19:30 belaban Exp $
 */
public class MemcachedConnector implements Runnable {
    private int port=11211;
    private InetAddress bind_addr=null;
    private PartitionedHashMap<String, Buffer> cache=null;
    private Thread thread=null;
    private ServerSocket srv_sock;
    private int core_threads=1, max_threads=100;
    private long idle_time=5000L;
    private Executor thread_pool;
    private long start_time;

    private final byte[] STORED="STORED\r\n".getBytes();
    private final byte[] DELETED="DELETED\r\n".getBytes();



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


    public Map<String, Object> getStats() {
        Map<String,Object> stats=new HashMap<String,Object>();
        stats.put("time", System.currentTimeMillis());
        stats.put("uptime", (System.currentTimeMillis() - start_time) / 1000L);
        return stats;
    }

    
    public void start() throws IOException {
        srv_sock=new ServerSocket(port, 50, bind_addr);
        if(thread_pool == null) {
            // thread_pool=new ThreadPoolExecutor(core_threads, max_threads, idle_time, TimeUnit.MILLISECONDS,
            //                               new LinkedBlockingQueue<Runnable>(100), new ThreadPoolExecutor.CallerRunsPolicy());
            thread_pool=new DirectExecutor();
        }
        if(thread == null || !thread.isAlive()) {
            thread=new Thread(this, "Acceptor");
            thread.start();
        }
        start_time=System.currentTimeMillis();
    }

    public void stop() throws IOException {
        Util.close(srv_sock);
        thread=null;
        if(thread_pool instanceof ExecutorService)
            ((ExecutorService)thread_pool).shutdown();
    }

    public void run() {
        System.out.println("MemcachedConnector listening on " + srv_sock.getLocalSocketAddress());
        while(thread != null && Thread.currentThread().equals(thread)) {
            Socket client_sock=null;
            try {
                client_sock=srv_sock.accept();
                RequestHandler handler=new RequestHandler(client_sock);
                thread_pool.execute(handler);
            }
            catch(ClosedSelectorException closed) {
                Util.close(client_sock);
                break;
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
    }


    private class RequestHandler implements Runnable {
        private final Socket client_sock;
        private final DataInputStream input;
        private final DataOutputStream output;




        public RequestHandler(Socket client_sock) throws IOException {
            this.client_sock=client_sock;
            this.input=new DataInputStream(client_sock.getInputStream());
            this.output=new DataOutputStream(client_sock.getOutputStream());
        }

        public void run() {
            Buffer val;
            while(client_sock.isConnected()) {
                try {
                    Request req=parseRequest(input);
                    if(req == null) {
                        output.write("CLIENT_ERROR failed to parse request\r\n".getBytes());
                        continue;
                    }

                    System.out.println("req = " + req);

                    switch(req.type) {
                        case SET:
                            byte[] data=new byte[req.number_of_bytes];
                            input.read(data, 0, data.length);
                            Util.readNewLine(input); // discard all input until after \r\n
                            val=new Buffer(data, 0, data.length);
                            cache.put(req.key, val, req.caching_time);
                            output.write(STORED);
                            break;

                        case GET:
                        case GETS:
                            if(req.get_key_list != null && !req.get_key_list.isEmpty()) {
                                for(String key: req.get_key_list) {
                                    val=cache.get(key);
                                    if(val != null) {
                                        int length=val.getLength();
                                        output.write(("VALUE " + key + " 0 " + length + "\r\n").getBytes());
                                        output.write(val.getBuf(), val.getOffset(), val.getLength());
                                        output.write(("\r\n").getBytes());
                                    }
                                }
                            }
                            Util.writeString(output, "END\r\n");
                            break;

                        case DELETE:
                            cache.remove(req.key);
                            output.write(DELETED);
                            break;

                        case STATS:
                            Map<String,Object> stats=getStats();
                            StringBuilder sb=new StringBuilder();
                            for(Map.Entry<String,Object> entry: stats.entrySet()) {
                                sb.append("STAT ").append(entry.getKey()).append(" ").append(entry.getValue()).append("\r\n");
                            }

                            sb.append("END\r\n");
                            output.write(sb.toString().getBytes());
                            break;
                    }
                }
                catch(IOException e) {
                }
            }
        }

        private Request parseRequest(DataInputStream in) throws IOException {
            Request req=new Request();
            String tmp=Util.parseString(in);
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
            else {
                return null;
            }

            switch(req.type) {
                case SET:
                case ADD:
                case REPLACE:
                case PREPEND:
                case APPEND:

                    // key
                    tmp=Util.parseString(in);
                    req.key=tmp;

                    // read flags and discard: flags are not supported
                    tmp=Util.parseString(in);

                    // expiry time
                    tmp=Util.parseString(in);
                    req.caching_time=Long.parseLong(tmp) * 1000L; // convert from secs to ms

                    // number of bytes
                    tmp=Util.parseString(in);
                    req.number_of_bytes=Integer.parseInt(tmp);

                    Util.readNewLine(in); // discard all input until after \r\n
                    break;
                case GET:
                case GETS:
                    req.get_key_list=new ArrayList<String>(5);
                    while(true) {
                        // key(s)
                        tmp=Util.parseString(in, true);
                        if(tmp == null || tmp.length() == 0)
                            break;
                        req.get_key_list.add(tmp);
                    }
                    break;

                case DELETE:
                    // key
                    tmp=Util.parseString(in);
                    req.key=tmp;
                    Util.readNewLine(in); // discard all input until after \r\n
                    break;

                case STATS:
                    Util.readNewLine(in); // discard all input until after \r\n
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

        public String toString() {
            return type + ": key=" + key + ", key_list=" + get_key_list +
                    ", caching_time=" + caching_time + ", number_of_bytes=" + number_of_bytes;
        }
    }
}

