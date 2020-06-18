package org.jgroups.demos;

import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Simple TCP server replying back to client with connection information (peer address etc)
 * @author Bela Ban
 * @since  4.1.8
 */
public class EchoServer {
    protected ServerSocket    srv_sock;
    protected ExecutorService thread_pool=Executors.newCachedThreadPool();
    protected final String    name;

    public EchoServer() {
        this.name=Util.generateLocalName();
    }

    protected void start(InetAddress bind_addr, int port) throws Exception {
        srv_sock=new ServerSocket(port, 50, bind_addr);
        System.out.printf("%s listening on %s\n", EchoServer.class.getSimpleName(), srv_sock.getLocalSocketAddress());
        while(!srv_sock.isClosed()) {
            try {
                Socket client_sock=srv_sock.accept();
                ConnectionHandler handler=new ConnectionHandler(client_sock);
                thread_pool.execute(handler);
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
        thread_pool.shutdown();
        thread_pool.awaitTermination(3, TimeUnit.SECONDS);
    }

    protected class ConnectionHandler implements Runnable {
        protected final Socket client_sock;

        public ConnectionHandler(Socket client_sock) {
            this.client_sock=client_sock;
        }

        public void run() {
            try(OutputStream output=client_sock.getOutputStream()) {
                System.out.printf("-- connection from %s\n", client_sock.getRemoteSocketAddress());
                String s=String.format("%s: running on %s\nclient: %s\n",
                                       name, InetAddress.getLocalHost().getHostName(),
                                       client_sock.getRemoteSocketAddress());
                output.write(s.getBytes());
                output.flush();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        protected void _run() throws Exception {
            try(OutputStream output=client_sock.getOutputStream();
                InputStream in=client_sock.getInputStream()) {
                String line=Util.readLine(in);
                System.out.printf("-- %s (from %s)\n", line, client_sock.getRemoteSocketAddress());
                String s=String.format("srv listening on %s (host: %s)\nclient: local=%s, remote=%s\n",
                                       srv_sock.getLocalSocketAddress(), InetAddress.getLocalHost().getHostName(),
                                       client_sock.getLocalSocketAddress(), client_sock.getRemoteSocketAddress());
                output.write(s.getBytes());
                output.flush();
            }
            catch(Throwable t) {
                // System.out.printf("health check by load balancer %s\n", client_sock.getRemoteSocketAddress());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        InetAddress bind_addr=null;
        int port=9000;
        for(int i=0; i < args.length; i++) {
            if(args[i].startsWith("-bind_addr")) {
                bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].startsWith("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("EchoServer [-bind_addr addr] [-port port] [-h]");
            return;
        }
        EchoServer es=new EchoServer();
        es.start(bind_addr, port);
    }


}
