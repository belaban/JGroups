package org.jgroups.client;

import org.jgroups.annotations.Experimental;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.STOMP;
import org.jgroups.util.Util;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
@Experimental
public class StompConnection implements Runnable {
    protected SocketFactory    socket_factory;
    protected Socket           sock;
    protected DataInputStream  in;
    protected DataOutputStream out;

    // collection of server addresses, we can pick any one to connect to
    protected final Set<String> server_destinations=new HashSet<>();

    protected final Set<Listener> listeners=new HashSet<>();

    protected final Set<String> subscriptions=new HashSet<>();

    protected final Set<ConnectionCallback> callbacks =new HashSet<>();

    protected Thread runner;

    protected volatile boolean running=false;

    protected String session_id;

    protected String userid;

    protected String password;

    protected boolean reconnect;

    protected final Log log=LogFactory.getLog(getClass());

    protected SSLParameters sslParameters;

    /**
     * @param dest IP address + ':' + port, e.g. "192.168.1.5:8787"
     */
    public StompConnection(String dest) {
        this(dest, null, null, false, false);
    }

    public StompConnection(String dest, boolean reconnect, boolean ssl) {
        this(dest, null, null, reconnect, ssl);
    }

    public StompConnection(String dest, boolean reconnect, SSLContext ssl) {
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
        this.sslParameters = null;
    }

    public StompConnection(String dest, String userid, String password, boolean reconnect, SSLContext sslcontext) {;
        server_destinations.add(dest);
        this.userid = userid;
        this.password = password;
        this.reconnect = reconnect;
        socket_factory = sslcontext.getSocketFactory();
        this.sslParameters = null;
    }

    public StompConnection(String dest, String userid, String password, boolean reconnect, SSLContext sslcontext,
                           SSLParameters sslParameters) {;
        server_destinations.add(dest);
        this.userid = userid;
        this.password = password;
        this.reconnect = reconnect;
        socket_factory = sslcontext.getSocketFactory();
        this.sslParameters = sslParameters;
    }

    public String getSessionId() {return session_id;}

    public void addListener(Listener listener) {
        if(listener != null)
            listeners.add(listener);
    }

    public void addCallback(ConnectionCallback cb) {
        if(cb != null)
            callbacks.add(cb);
    }

    public void removeListener(Listener listener) {
        if(listener != null)
            listeners.remove(listener);
    }

    public void removeCallback(ConnectionCallback cb) {
        if(cb != null)
            callbacks.remove(cb);
    }

    protected synchronized void startRunner() {
        if(runner == null || !runner.isAlive()) {
            running = true;
            runner=new Thread(this, "StompConnection receiver");
            runner.start();
        }
    }

    protected void sendConnect() throws IOException {
        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.CONNECT.name()).append("\n");
        if(userid != null)
            sb.append("login: ").append(userid).append("\n");
        if(password != null)
            sb.append("passcode: ").append(password).append("\n");
        sb.append("\n");

        try {
            synchronized(this) {
                out.write(sb.toString().getBytes());
                out.write(STOMP.NULL_BYTE);
                out.flush();
            }
        }
        catch(IOException ex) {
            log.error(Util.getMessage("FailedToSendConnectMessage"), ex);
            throw ex;
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
        sb.append(STOMP.ClientVerb.SUBSCRIBE.name()).append("\n").append("destination: ")
                .append(destination).append("\n\n");

        try {
            synchronized(this) {
                out.write(sb.toString().getBytes());
                out.write(STOMP.NULL_BYTE);
                out.flush();
            }
        }
        catch(IOException ex) {
            log.error(Util.getMessage("FailedSubscribingTo") + destination + ": ", ex);
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
        sb.append(STOMP.ClientVerb.UNSUBSCRIBE.name()).append("\n")
                .append("destination: ").append(destination).append("\n\n");

        try {
            synchronized(this) {
                out.write(sb.toString().getBytes());
                out.write(STOMP.NULL_BYTE);
                out.flush();
            }
        }
        catch(IOException ex) {
            log.error(Util.getMessage("FailedUnsubscribingFrom") + destination + ": ", ex);
        }
    }

    public void send(String destination, byte[] buf, int offset, int length, String ... headers) {
        StringBuilder sb=new StringBuilder();
        sb.append(STOMP.ClientVerb.SEND.name()).append("\n");
        if(destination != null)
            sb.append("destination: ").append(destination).append("\n");
        if(buf != null)
            sb.append("content-length: ").append(length).append("\n");
        if(headers != null && (headers.length & 1) == 0) { // must be even
            for(int i=0; i < headers.length; i++)
                sb.append(headers[i]).append(": ").append(headers[++i]).append("\n");
        }
        sb.append("\n");

        try {
            synchronized(this) {
                out.write(sb.toString().getBytes());
                if(buf != null)
                    out.write(buf, offset, length);
                out.write(STOMP.NULL_BYTE);
                out.flush();
            }
        }
        catch (IOException e) {
            log.error(Util.getMessage("FailedSendingMessageTo") + destination + ": ", e);
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
        int timeout = 0;
        while(running) {
            try {
                if (!isConnected() && reconnect) {
                    log.info("Reconnecting in "+timeout+"s.");
                    try {
                        Thread.sleep((long)timeout * 1000);
                    }
                    catch (InterruptedException e1) {
                        // pass
                    }
                    timeout = timeout*2 > 60 ? 60 : (timeout+1)*2;

                    try {
                        connect();
                    }
                    catch (IOException e) {
                        // continue and attempt reconnect
                        continue;
                    }

                    callbacks.forEach(ConnectionCallback::onConnect);
                }

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

                // reset the connection backoff when we successfully connect.
                timeout = 0;
            }
            catch(IOException e) {
                if (running) {
                    // only unexpected if running is true, otherwise disconnect was already called
                    log.error(Util.getMessage("ConnectionClosedUnexpectedly"), e);
                }
                if (reconnect) {
                    closeConnections();
                }
                else {
                    disconnect();
                }
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailureReadingFrame"), t);
            }
        }
    }

    protected void notifyListeners(Map<String,String> headers, byte[] buf, int offset, int length) {
        for(Listener listener: listeners) {
            try {
                listener.onMessage(headers, buf, offset, length);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedCallingListener"), t);
            }
        }
    }

    protected void notifyListeners(Map<String,String> info) {
        for(Listener listener: listeners) {
            try {
                listener.onInfo(info);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedCallingListener"), t);
            }
        }
    }

    public void connectToSingleDestination(String destination) throws IOException {
        try {
            synchronized(this) {
                connectToDestination(destination);
                sendConnect();
            }
            subscriptions.forEach(this::sendSubscribe);
        }
        catch(IOException ex) {
            closeConnections();
            throw ex;
        }

    }

    public void connect() throws IOException{
        for (String dest : server_destinations) {
            try {
                connectToSingleDestination(dest);
                log.info("Connected to " + dest);
                break;
            }
            catch(IOException ex) {
                if(log.isErrorEnabled())
                    log.error(Util.getMessage("FailedConnectingTo") + dest + ":" + ex);
            }
        }

        if(!isConnected())
            throw new IOException("no target server available");

        startRunner();
    }

    public void startReconnectingClient() {
        startRunner();
    }

    protected Socket buildSocket(String host, int port) throws IOException {
        // both SocketFactory and SSLSocketFactory return abstract class Socket
        // on createSocket calls, unfortunately we need to configure
        // SSLSocket with SSLParameters, so we need to check if the socket is
        // and instance of SSLSocket or not before we cast and modify
        sock=socket_factory.createSocket(host, port);

        if (sock instanceof SSLSocket && this.sslParameters != null) {
            ((SSLSocket) sock).setSSLParameters(this.sslParameters);
        }

        return sock;
    }

    protected void connectToDestination(String dest) throws IOException {
        // parse destination
        int index=dest.lastIndexOf(':');
        String host=dest.substring(0, index);
        int port=Integer.parseInt(dest.substring(index+1));

        sock=buildSocket(host, port);

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

    public interface Listener {
        void onMessage(Map<String,String> headers, byte[] buf, int offset, int length);
        void onInfo(Map<String,String> information);
    }

    public interface ConnectionCallback {
        void onConnect();
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
        StompConnection conn=new StompConnection(host+ ":" + port, true, false);
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
