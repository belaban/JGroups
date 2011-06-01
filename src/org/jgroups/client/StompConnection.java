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
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
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
 */
@Experimental @Unsupported
public class StompConnection implements Runnable {
    protected SocketFactory    socket_factory;
    protected Socket           sock;
    protected DataInputStream  in;
    protected DataOutputStream out;

    // collection of server addresses, we can pick any one to connect to
    protected final Set<String> server_destinations=new HashSet<String>();

    protected final Set<Listener> listeners=new HashSet<Listener>();

    protected final Set<String> subscriptions=new HashSet<String>();

    protected Thread runner;

    protected volatile boolean running=false;

    protected String session_id;

    protected String userid;

    protected String password;

    protected boolean reconnect;

    protected final Log log=LogFactory.getLog(getClass());

    /**
     * @param dest IP address + ':' + port, e.g. "192.168.1.5:8787"
     */
    public StompConnection(String dest) {
        this(dest, null, null, false, false);
    }

    public StompConnection(String dest, boolean reconnect, boolean ssl) {
        this(dest, null, null, reconnect, ssl);
    }

    public StompConnection(String dest, String userid, String password, boolean reconnect, boolean ssl) {;
        server_destinations.add(dest);
        this.userid = userid;
        this.password = password;
        this.reconnect = reconnect;
        if (ssl)
            socket_factory = SSLSocketFactory.getDefault();
        else
            socket_factory = SocketFactory.getDefault();
    }

    public String getSessionId() {return session_id;}

    public void addListener(Listener listener) {
        if(listener != null)
            listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        if(listener != null)
            listeners.remove(listener);
    }

    protected synchronized void startRunner() {
        if(runner == null || !runner.isAlive()) {
            running = true;
            runner=new Thread(this, "StompConnection receiver");
            runner.start();
        }
    }

    protected void sendConnect() {
        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.CONNECT.name()).append("\n");
        if(userid != null)
            sb.append("login: ").append(userid).append("\n");
        if(password != null)
            sb.append("passcode: ").append(password).append("\n");
        sb.append("\n");

        try {
            out.write(sb.toString().getBytes());
            out.write(STOMP.NULL_BYTE);
            out.flush();
        }
        catch(IOException ex) {
            log.error("failed to send connect message:", ex);
        }
    }

    public void subscribe(String destination) {
        if(destination == null)
            return;
        subscriptions.add(destination);

        if(isConnected()) {
            sendSubscribe(destination);
        }
    }

    protected void sendSubscribe(String destination) {
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
            log.error("failed subscribing to " + destination + ": ", ex);
        }
    }

    public void unsubscribe(String destination) {
        if(destination == null)
            return;
        subscriptions.remove(destination);

        if(isConnected()) {
            sendUnsubscribe(destination);
        }
    }

    protected void sendUnsubscribe(String destination) {
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
            log.error("failed unsubscribing from " + destination + ": ", ex);
        }
    }

    public void send(String destination, byte[] buf, int offset, int length, String ... headers) {
        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.SEND.name()).append("\n");
        if(destination != null)
            sb.append("destination: ").append(destination).append("\n");
        if(buf != null)
            sb.append("content-length: ").append(length).append("\n");
        if(headers != null && headers.length % 2 == 0) { // must be even
            for(int i=0; i < headers.length; i++)
                sb.append(headers[i]).append(": ").append(headers[++i]).append("\n");
        }
        sb.append("\n");

        try {
            out.write(sb.toString().getBytes());
            if(buf != null)
                out.write(buf, offset, length);
            out.write(STOMP.NULL_BYTE);
            out.flush();
        }
        catch (IOException e) {
            log.error("failed sending message to " + destination + ": ", e);
        }
    }

    /**
     * Sends an INFO without body
     */
    public void send(String destination, String ... headers) {
        send(destination, null, 0, 0, headers);
    }

    public void send(String destination, byte[] buf, int offset, int length) {
        send(destination, buf, offset, length, (String[])null);
    }

    public void send(String destination, byte[] buf) {
        send(destination, buf, 0, buf.length);
    }

    public void run() {
        int timeout = 1;
        while(running) {
            try {
                if (!isConnected() && reconnect) {
                    log.error("Reconnecting in "+timeout+"s.");
                    try {
                        Thread.sleep(timeout * 1000);
                    }
                    catch (InterruptedException e1) {
                        // pass
                    }
                    timeout = timeout*2 > 60 ? 60 : timeout*2;

                    connect();
                }

                // reset the connection backoff when we successfully connect.
                timeout = 1;

                STOMP.Frame frame=STOMP.readFrame(in);
                if(frame != null) {
                    STOMP.ServerVerb verb=STOMP.ServerVerb.valueOf(frame.getVerb());
                    if(log.isTraceEnabled())
                        log.trace("frame: " + frame);
                    switch(verb) {
                        case MESSAGE:
                            byte[] buf=frame.getBody();
                            notifyListeners(frame.getHeaders(), buf, 0, buf != null? buf.length : 0);
                            break;
                        case CONNECTED:
                            String sess_id=frame.getHeaders().get("session-id");
                            if(sess_id != null) {
                                this.session_id=sess_id;
                            }
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
                log.error("Connection closed unexpectedly:", e);
                if (reconnect) {
                    closeConnections();
                }
                else {
                    disconnect();
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

    public void connect() throws IOException{
        for (String dest : server_destinations) {
            try {
                connectToDestination(dest);
                sendConnect();
                for(String subscription: subscriptions)
                     sendSubscribe(subscription);
                if(log.isDebugEnabled())
                    log.debug("connected to " + dest);
                break;
            }
            catch(IOException ex) {
                if(log.isErrorEnabled())
                    log.error("failed connecting to " + dest, ex);
                closeConnections();
            }
        }

        if(!isConnected())
            throw new IOException("no target server available");

        startRunner();
    }

    public void startReconnectingClient() {
        startRunner();
    }

    protected void connectToDestination(String dest) throws IOException {
        // parse destination
        int index=dest.lastIndexOf(":");
        String host=dest.substring(0, index);
        int port=Integer.parseInt(dest.substring(index+1));

        sock=socket_factory.createSocket(host, port);

        in=new DataInputStream(sock.getInputStream());
        out=new DataOutputStream(sock.getOutputStream());
    }

    public void disconnect() {
        running = false;
        closeConnections();
    }

    protected void closeConnections() {
        Util.close(in);
        Util.close(out);
        Util.close(sock);
    }

    public boolean isConnected() {
        return sock != null && sock.isConnected() && !sock.isClosed();
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
                else if(line.startsWith("disconnect")) {
                    conn.disconnect();
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
