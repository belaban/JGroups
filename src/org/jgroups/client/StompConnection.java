package org.jgroups.client;

import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * STOMP client to access the STOMP protocol
 * @author Bela Ban
 * @version $Id: StompConnection.java,v 1.1 2010/10/26 06:46:51 belaban Exp $
 */
public class StompConnection {
    protected Socket           sock;
    protected DataInputStream  in;
    protected DataOutputStream out;

    // collection of server addresses, we can pick any one to connect to
    protected final Set<String> server_destinations=new HashSet<String>();

    protected final Set<Listener> listeners=new HashSet<Listener>();

    /**
     *
     * @param dest IP address + ':' + port, e.g. "192.168.1.5:8787"
     */
    public StompConnection(String dest) {
        server_destinations.add(dest);
    }

    public void addListener(Listener listener) {
        if(listener != null)
            listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        if(listener != null)
            listeners.add(listener);
    }


    public void connect(String userid, String password) throws IOException {
        String dest;

        while((dest=pickRandomDestination()) != null) {
            try {
                connect(dest);
                return;
            }
            catch(IOException ex) {
                close();
                server_destinations.remove(dest);
            }
        }
        throw new IOException("no target server available");
    }



    public void connect() throws IOException {
        connect(null, null);
    }

    public void disconnect() {

    }

    public void subscribe(String destination) {

    }

    public void unsubscribe(String destination) {

    }

    public void send(String destination, byte[] buf, int offset, int length) {

    }

    public void send(String destination, byte[] buf) {
        send(destination, buf, 0, buf.length);
    }

    
    protected String pickRandomDestination() {
        return server_destinations.isEmpty()? null : server_destinations.iterator().next();
    }

    protected void connect(String dest) throws IOException {
        SocketAddress saddr=parse(dest);
        sock=new Socket();
        sock.connect(saddr);
        in=new DataInputStream(sock.getInputStream());
        out=new DataOutputStream(sock.getOutputStream());
    }

    protected static SocketAddress parse(String dest) throws UnknownHostException {
        int index=dest.lastIndexOf(":");
        String host=dest.substring(0, index);
        int port=Integer.parseInt(dest.substring(index+1));
        return new InetSocketAddress(host, port);
    }

    protected void close() {
        Util.close(in);
        Util.close(out);
        Util.close(sock);
    }

    protected boolean isConnected() {
        return sock != null && sock.isConnected();
    }


    public static interface Listener {
        void onMessage(byte[] buf, int offset, int length);
        void onInfo(Map<String,String> information);
    }


    public static void main(String[] args) throws IOException {
        String host="localhost";
        String port="8787";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-h")) {
                host=args[++i];
                continue;
            }
            if(args[i].equals("-p")) {
                port=args[++i];
                continue;
            }
            System.out.println("StompConnection [-h host] [-p port]");
            return;
        }
        StompConnection conn=new StompConnection(host+ ":" + port);
        conn.connect();
        
    }
}
