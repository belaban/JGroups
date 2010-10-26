package org.jgroups.client;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.STOMP;
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
 * @version $Id: StompConnection.java,v 1.2 2010/10/26 12:30:16 belaban Exp $
 */
public class StompConnection implements Runnable {
    protected Socket           sock;
    protected DataInputStream  in;
    protected DataOutputStream out;

    // collection of server addresses, we can pick any one to connect to
    protected final Set<String> server_destinations=new HashSet<String>();

    protected final Set<Listener> listeners=new HashSet<Listener>();

    protected final Set<String> subscriptions=new HashSet<String>();

    protected Thread runner;

    protected final Log log=LogFactory.getLog(getClass());


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

        if(isConnected())
            return;
        while((dest=pickRandomDestination()) != null) {
            try {
                connect(dest);
                break;
            }
            catch(IOException ex) {
                close();
                server_destinations.remove(dest);
            }
        }
        if(!isConnected())
            throw new IOException("no target server available");

        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.CONNECT.name()).append("\n");
        if(userid != null)
            sb.append("login: ").append(userid).append("\n");
        if(password != null)
            sb.append("passcode: ").append(password).append("\n");
        sb.append("\n").append(STOMP.NULL_BYTE);

        out.write(sb.toString().getBytes());
        out.flush();
    }


    public void reconnect() throws IOException {
        connect();
        for(String subscription: subscriptions)
            subscribe(subscription);
    }



    public void connect() throws IOException {
        connect(null, null);
    }

    public void disconnect() {

    }

    public void subscribe(String destination) {
        if(destination == null)
            return;
        subscriptions.add(destination);
    }

    public void unsubscribe(String destination) {
        if(destination == null)
            return;
        subscriptions.remove(destination);
    }

    public void send(String destination, byte[] buf, int offset, int length) {

    }

    public void send(String destination, byte[] buf) {
        send(destination, buf, 0, buf.length);
    }

    public void run() {
        while(isConnected()) {
            try {
                STOMP.Frame frame=STOMP.readFrame(in);
                if(frame != null) {
                    STOMP.ServerVerb verb=STOMP.ServerVerb.valueOf(frame.getVerb());
                    System.out.println("frame = " + frame);
                    switch(verb) {
                        case MESSAGE:
                            byte[] buf=frame.getBody();
                            notifyListeners(frame.getHeaders(), buf, 0, buf != null? buf.length : 0);
                            break;
                        case CONNECTED:
                            break;
                        case ERROR:
                            break;
                        case INFO:
                            break;
                        case RECEIPT:
                            break;
                        default:
                            throw new IllegalArgumentException("verb " + verb + " is not known");
                    }
                }
            }
            catch(IOException e) {
                close();
                try {
                    reconnect();
                }
                catch(IOException e1) {
                    log.warn("failed to reconnect; runner thread terminated");
                }
            }
            catch(Throwable t) {
                log.error("failure reading frame", t);
            }
        }
    }

    protected void notifyListeners(Map<String,String> headers, byte[] buf, int offset, int length) {
        for(Listener listener: listeners) {
            try {
                listener.onMessage(headers, buf, offset, length);
            }
            catch(Throwable t) {
                log.error("failed calling listener", t);
            }
        }
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
        startRunner();
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

    protected synchronized void startRunner() {
        if(runner == null || !runner.isAlive()) {
            runner=new Thread(this, "StompConnection receiver");
            runner.start();
        }
    }



    public static interface Listener {
        void onMessage(Map<String,String> headers, byte[] buf, int offset, int length);
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
        conn.addListener(new Listener() {

            public void onMessage(Map<String, String> headers, byte[] buf, int offset, int length) {
                System.out.println("<< " + new String(buf, offset, length) + ", headers: " + headers);
            }

            public void onInfo(Map<String, String> information) {
            }
        });
        conn.connect();

        for(;;) {
            
        }
    }
}
