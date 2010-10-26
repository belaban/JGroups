package org.jgroups.client;

import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.STOMP;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;

/**
 * STOMP client to access the STOMP [1] protocol. Note that the full STOMP protocol is not implemented, e.g. transactions
 * are currently not supported.
 * <p/>
 * The interactive client can be started with -h HOST -p PORT, which are the hostname and port of a JGroups server, running
 * with STOMP in its stack configuration. The interactive client supports automatic failover to a different server if
 * the currently connected-to server crashes, and a simple syntax for sending STOMP messages:
 * <pre>
 * subscribe DEST // example: subscribe /topics/a
 * send DEST message // example: send /topics/a Hello world
 * </pre>
 * <p/>
 * [1] http://stomp.codehaus.org/Protocol
 * @author Bela Ban
 * @version $Id: StompConnection.java,v 1.3 2010/10/26 16:08:42 belaban Exp $
 */
@Experimental @Unsupported
public class StompConnection implements Runnable {
    protected Socket           sock;
    protected DataInputStream  in;
    protected DataOutputStream out;

    // collection of server addresses, we can pick any one to connect to
    protected final Set<String> server_destinations=new HashSet<String>();

    protected final Set<Listener> listeners=new HashSet<Listener>();

    protected final Set<String> subscriptions=new HashSet<String>();

    protected Thread runner;

    protected volatile boolean running=true;

    protected final Log log=LogFactory.getLog(getClass());


    /**
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
        sb.append("\n");

        out.write(sb.toString().getBytes());
        out.write(STOMP.NULL_BYTE);
        out.flush();
    }


    public void reconnect() throws IOException {
        if(!running)
            return;
        connect();
        for(String subscription: subscriptions)
            subscribe(subscription);
        if(log.isDebugEnabled()) {
            log.debug("reconnected to " + sock.getInetAddress().getHostAddress() + ":" + sock.getPort());
            if(!subscriptions.isEmpty())
                log.debug("re-subscribed to " + subscriptions);
        }

    }



    public void connect() throws IOException {
        connect(null, null);
    }

    public void disconnect() {
        running=false;
        close();
    }

    public void subscribe(String destination) {
        if(destination == null)
            return;
        subscriptions.add(destination);

        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.SUBSCRIBE.name()).append("\n");
        sb.append("destination: ").append(destination).append("\n");
        sb.append("\n");

        try {
            out.write(sb.toString().getBytes());
            out.write(STOMP.NULL_BYTE);
            out.flush();
        }
        catch(IOException ex) {
            log.error("failed subscribing to " + destination + ": " + ex);
        }
    }

    public void unsubscribe(String destination) {
        if(destination == null)
            return;
        subscriptions.remove(destination);

        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.UNSUBSCRIBE.name()).append("\n");
        sb.append("destination: ").append(destination).append("\n");
        sb.append("\n");

        try {
            out.write(sb.toString().getBytes());
            out.write(STOMP.NULL_BYTE);
            out.flush();
        }
        catch(IOException ex) {
            log.error("failed unsubscribing from " + destination + ": " + ex);
        }
    }

    public void send(String destination, byte[] buf, int offset, int length) {
        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.SEND.name()).append("\n");
        if(destination != null)
            sb.append("destination: ").append(destination).append("\n");
        if(buf != null)
            sb.append("content-length: ").append(length +1).append("\n"); // the 1 additional byte is the NULL_BYTE
        sb.append("\n");

        try {
            out.write(sb.toString().getBytes());
            out.write(buf, offset, length);
            out.write(STOMP.NULL_BYTE);
            out.flush();
        }
        catch(IOException ex) {
            log.error("failed sending message to server: " + ex);
        }
    }

    public void send(String destination, byte[] buf) {
        send(destination, buf, 0, buf.length);
    }

    public void run() {
        while(isConnected() && running) {
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
                            notifyListeners(frame.getHeaders());
                            String endpoints=frame.getHeaders().get("endpoints");
                            if(endpoints != null) {
                                List<String> list=Util.parseCommaDelimitedStrings(endpoints);
                                if(list != null) {
                                    boolean changed=server_destinations.addAll(list);
                                    if(changed && log.isDebugEnabled())
                                        log.debug("INFO: new server target list: " + server_destinations);
                                }
                            }
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

    protected void notifyListeners(Map<String,String> info) {
        for(Listener listener: listeners) {
            try {
                listener.onInfo(info);
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
        return sock != null && sock.isConnected() && !sock.isClosed();
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
                System.out.println("<< INFO: " + information);
            }
        });
        conn.connect();

        while(conn.isConnected()) {
            try {
                String line=Util.readStringFromStdin(": ");
                if(line.startsWith("subscribe")) {
                    String dest=line.substring("subscribe".length()).trim();
                    conn.subscribe(dest);
                }
                else if(line.startsWith("unsubscribe")) {
                    String dest=line.substring("unsubscribe".length()).trim();
                    conn.unsubscribe(dest);
                }
                else if(line.startsWith("send")) {
                    String rest=line.substring("send".length()).trim();

                    int index=rest.indexOf(' ');
                    if(index != -1) {
                        String dest=rest.substring(0, index);
                        String body=rest.substring(index+1);
                        byte[] buf=body.getBytes();
                        conn.send(dest, buf, 0, buf.length);
                    }
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
