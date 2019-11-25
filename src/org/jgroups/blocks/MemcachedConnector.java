package org.jgroups.blocks;

import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.Util;

import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedSelectorException;
import java.util.*;
import java.util.concurrent.*;

/** Class which listens on a server socket for memcached clients, reads the requests, forwards them to an instance of
 * PartitionedHashMap and sends the response. A memcached client should be able to work without changes once the
 * memcached protocol (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt) has been implemented
 * completely.<br/>
 * @author Bela Ban
 */
public class MemcachedConnector implements Runnable {
    @ManagedAttribute()
    private int port=11211;
    @ManagedAttribute()
    private InetAddress bind_addr=null;
    private PartitionedHashMap<String, byte[]> cache=null;
    private Thread thread=null;
    private ServerSocket srv_sock;

    @ManagedAttribute(writable=true)
    private int core_threads=1;

    @ManagedAttribute(writable=true)
    private int max_threads=500;

    @ManagedAttribute(writable=true)
    private long idle_time=5000L;

    private Executor thread_pool;
    private long start_time;

    private final byte[] STORED="STORED\r\n".getBytes();
    private final byte[] DELETED="DELETED\r\n".getBytes();
    private final byte[] END="END\r\n".getBytes();
    private final byte[] RN="\r\n".getBytes();



    public MemcachedConnector(InetAddress bind_addr, int port, PartitionedHashMap<String, byte[]> cache) {
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

    public PartitionedHashMap<String, byte[]> getCache() {
        return cache;
    }

    public void setCache(PartitionedHashMap<String, byte[]> cache) {
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
        Map<String,Object> stats=new HashMap<>();
        stats.put("time", System.currentTimeMillis());
        stats.put("uptime", (System.currentTimeMillis() - start_time) / 1000L);
        return stats;
    }


    @ManagedOperation
    public void start() throws IOException, MalformedObjectNameException, MBeanRegistrationException {
        srv_sock=new ServerSocket(port, 50, bind_addr);
        if(thread_pool == null) {
            thread_pool=new ThreadPoolExecutor(core_threads, max_threads, idle_time, TimeUnit.MILLISECONDS,
                                               new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
            // thread_pool=new DirectExecutor();
        }
        if(thread == null || !thread.isAlive()) {
            thread=new Thread(this, "Acceptor");
            thread.start();
        }
        start_time=System.currentTimeMillis();
    }

    @ManagedOperation
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
                // System.out.println("ACCEPT: " + client_sock.getRemoteSocketAddress());
                final RequestHandler handler=new RequestHandler(client_sock);
                /*new Thread() {
                    public void run() {
                        handler.run();
                    }
                }.start();
                */
                thread_pool.execute(handler);
            }
            catch(ClosedSelectorException closed) {
                Util.close(client_sock);
                break;
            }
            catch(Throwable e) {
            }
        }
    }


    private class RequestHandler implements Runnable {
        private final Socket client_sock;
        private final InputStream input;
        private final OutputStream output;


        public RequestHandler(Socket client_sock) throws IOException {
            this.client_sock=client_sock;
            this.input=new BufferedInputStream(client_sock.getInputStream());
            this.output=new BufferedOutputStream(client_sock.getOutputStream());
        }

        public void run() {
            byte[] val;
            String line;
            
            while(client_sock.isConnected()) {
                try {
                    line=Util.readLine(input);
                    if(line == null)
                        break;

                    // System.out.println("line = " + line);

                    Request req=parseRequest(line);
                    if(req == null) {
                        break;
                    }

                    // System.out.println("req = " + req);

                    switch(req.type) {
                        case SET:
                            byte[] data=new byte[req.number_of_bytes];
                            int num=input.read(data, 0, data.length);
                            if(num == -1)
                                throw new EOFException();
                            cache.put(req.key, data, req.caching_time);
                            output.write(STORED);
                            output.flush();
                            Util.discardUntilNewLine(input);
                            break;

                        case GET:
                        case GETS:
                            if(req.keys != null && !req.keys.isEmpty()) {
                                for(String key: req.keys) {
                                    val=cache.get(key);
                                    if(val != null) {
                                        int length=val.length;
                                        output.write(("VALUE " + key + " 0 " + length + "\r\n").getBytes());
                                        output.write(val, 0, length);
                                        output.write(RN);
                                    }
                                }
                            }
                            output.write(END);
                            output.flush();
                            break;

                        case DELETE:
                            cache.remove(req.key);
                            output.write(DELETED);
                            output.flush();
                            break;

                        case STATS:
                            Map<String,Object> stats=getStats();
                            StringBuilder sb=new StringBuilder();
                            for(Map.Entry<String,Object> entry: stats.entrySet()) {
                                sb.append("STAT ").append(entry.getKey()).append(" ").append(entry.getValue()).append("\r\n");
                            }

                            sb.append("END\r\n");
                            output.write(sb.toString().getBytes());
                            output.flush();
                            break;
                    }
                }
                catch(StreamCorruptedException corrupted_ex) {
                    try {
                        output.write(("CLIENT_ERROR failed to parse request: " + corrupted_ex + ":\r\n").getBytes());
                        output.flush();
                    }
                    catch(IOException e) {}
                }
                catch(EOFException end_of_file_ex) {
                    break;
                }
                catch(Throwable e) {
                }
            }
            Util.close(client_sock);
        }

        private Request parseRequest(String line) throws IOException {
            Request req=new Request();
            String[] args=line.trim().split(" +");
            String tmp=args[0];
            if(tmp == null)
                throw new EOFException();
            switch(tmp) {
                case "set":
                    req.type=Request.Type.SET;
                    break;
                case "add":
                    req.type=Request.Type.ADD;
                    break;
                case "replace":
                    req.type=Request.Type.REPLACE;
                    break;
                case "prepend":
                    req.type=Request.Type.PREPEND;
                    break;
                case "append":
                    req.type=Request.Type.APPEND;
                    break;
                case "cas":
                    req.type=Request.Type.CAS;
                    break;
                case "incr":
                    req.type=Request.Type.INCR;
                    break;
                case "decr":
                    req.type=Request.Type.DECR;
                    break;
                case "get":
                    req.type=Request.Type.GET;
                    break;
                case "gets":
                    req.type=Request.Type.GETS;
                    break;
                case "delete":
                    req.type=Request.Type.DELETE;
                    break;
                case "stat":
                    req.type=Request.Type.STAT;
                    break;
                case "stats":
                    req.type=Request.Type.STATS;
                    break;
                default:
                    throw new StreamCorruptedException("request \"" + line + "\" not known");
            }

            switch(req.type) {
                case SET:
                case ADD:
                case REPLACE:
                case PREPEND:
                case APPEND:

                    // key
                    tmp=args[1];
                    if(tmp == null)
                        throw new EOFException();
                    req.key=tmp;

                    // read flags and discard: flags are not supported
                    tmp=args[2];
                    if(tmp == null)
                        throw new EOFException();

                    // expiry time
                    tmp=args[3];
                    if(tmp == null)
                        throw new EOFException();
                    req.caching_time=Long.parseLong(tmp) * 1000L; // convert from secs to ms

                    // number of bytes
                    tmp=args[4];
                    if(tmp == null)
                        throw new EOFException();
                    req.number_of_bytes=Integer.parseInt(tmp);
                    break;
                case GET:
                case GETS:
                    req.keys=new ArrayList<>(5);
                    req.keys.addAll(Arrays.asList(args).subList(1, args.length));
                    break;

                case DELETE:
                    // key
                    tmp=args[1];
                    if(tmp == null)
                        throw new EOFException();
                    req.key=tmp;
                    break;

                case STATS:
                    break;
            }

            return req;
        }


    }


    public static class Request {
        public enum Type {SET, ADD, REPLACE, PREPEND, APPEND, CAS, INCR, DECR, GET, GETS, DELETE, STAT, STATS};

        Type type;
        String key;
        List<String> keys=null;
        long caching_time;
        int number_of_bytes=0;

        public Request() {
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type + ": ");
            if(key != null)
                sb.append("key=" + key);
            else if(keys != null && !keys.isEmpty())
                sb.append("keys=" + keys);
            sb.append(", caching_time=" + caching_time + ", number_of_bytes=" + number_of_bytes);
            return sb.toString();
        }
    }
}

