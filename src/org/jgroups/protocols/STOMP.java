package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Protocol which provides STOMP support. Very simple implementation, with a 1 thread / connection model. Use for
 * a few hundred clients max.
 * @author Bela Ban
 * @version $Id: STOMP.java,v 1.6 2010/10/22 13:23:52 belaban Exp $
 * @since 2.11
 */
@MBean
@Experimental @Unsupported
public class STOMP extends Protocol implements Runnable {

    /* -----------------------------------------    Properties     ----------------------------------------------- */
    @Property(description="Port on which the STOMP protocol listens for requests",writable=false)
    protected int port=8787;


    /* ---------------------------------------------   JMX      ---------------------------------------------------*/
    @ManagedAttribute(description="Number of client connections",writable=false)
    public int getNumConnections() {return connections.size();}

    @ManagedAttribute(description="Number of subscriptions",writable=false)
    public int getNumSubscriptions() {return subscriptions.size();}

    @ManagedAttribute(description="Print subscriptions",writable=false)
    public String getSubscriptions() {return subscriptions.keySet().toString();}


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected ServerSocket           srv_sock;
    protected Thread                 acceptor;
    protected final List<Connection> connections=new LinkedList<Connection>();

    // Subscriptions and connections which are subscribed
    protected final ConcurrentMap<String, Set<Connection>> subscriptions=Util.createConcurrentMap(20);



    public static enum ClientVerb      {CONNECT, SUBSCRIBE, UNSUBSCRIBE, BEGIN, COMMIT, ABORT, ACK, DISCONNECT};
    public static enum ServerVerb      {MESSAGE, RECEIPT, ERROR}
    public static enum ServerResponse  {CONNECTED}

    
    public STOMP() {
    }



    public void start() throws Exception {
        super.start();
        srv_sock=Util.createServerSocket(getSocketFactory(), Global.STOMP_SRV_SOCK, port);
        if(log.isDebugEnabled())
            log.debug("server socket listening on " + srv_sock.getLocalSocketAddress());

        if(acceptor == null) {
            acceptor=getThreadFactory().newThread(this, "STOMP acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }
    }


    public void stop() {
        if(log.isDebugEnabled())
            log.debug("closing server socket " + srv_sock.getLocalSocketAddress());

        if(acceptor != null && acceptor.isAlive()) {
            try {
                // this will terminate thread, peer will receive SocketException (socket close)
                getSocketFactory().close(srv_sock);
            }
            catch(Exception ex) {
            }
        }
        synchronized(connections) {
            for(Connection conn: connections)
                conn.stop();
            connections.clear();
        }
        acceptor=null;
        super.stop();
    }

    // Acceptor loop
    public void run() {
        Socket client_sock;
        while(acceptor != null && srv_sock != null) {
            try {
                client_sock=srv_sock.accept();
                if(log.isTraceEnabled())
                    log.trace("accepted connection from " + client_sock.getInetAddress() + ':' + client_sock.getPort());
                Connection conn=new Connection(client_sock);
                Thread thread=getThreadFactory().newThread(conn, "STOMP client connection");
                thread.setDaemon(true);

                synchronized(connections) {
                    connections.add(conn);
                }
                thread.start();
            }
            catch(IOException io_ex) {
                break;
            }
        }
        acceptor=null;
    }


    /**
     * Class which handles a connection to a client
     */
    protected class Connection implements Runnable {
        protected final Socket sock;
        protected final DataInputStream in;
        protected final DataOutputStream out;
        protected final UUID session_id=UUID.randomUUID();

        public Connection(Socket sock) throws IOException {
            this.sock=sock;
            this.in=new DataInputStream(sock.getInputStream());
            this.out=new DataOutputStream(sock.getOutputStream());
        }

        public void stop() {
            if(log.isTraceEnabled())
                log.trace("closing connection to " + sock.getRemoteSocketAddress());
            Util.close(in);
            Util.close(out);
            Util.close(sock);
        }

        protected void remove() {
            synchronized(connections) {
                connections.remove(this);
            }
        }

        public void run() {
            while(!sock.isClosed()) {
                try {
                    Frame frame=readFrame(in);
                    if(frame != null) {
                        if(log.isTraceEnabled())
                            log.trace(frame);
                        handleFrame(frame);
                    }
                }
                catch(EOFException eof_ex) {
                    stop();
                    remove();
                }
                catch(IOException ex) {
                    log.error("failure reading frame", ex);
                    stop();
                    remove();
                }
                catch(Throwable t) {
                    log.error("failure reading frame", t);
                }
            }
        }


        protected void handleFrame(Frame frame) {
            switch(frame.getVerb()) {
                case CONNECT:
                    writeResponse(ServerResponse.CONNECTED,
                                  "session-id", session_id.toString(),
                                  "password-check", "none");
                    break;
                case SUBSCRIBE:
                    Map<String,String> headers=frame.getHeaders();
                    String destination=headers.get("destination");
                    if(destination != null) {
                        Set<Connection> conns=subscriptions.get(destination);
                        if(conns == null) {
                            conns=new HashSet<Connection>();
                            Set<Connection> tmp=subscriptions.putIfAbsent(destination, conns);
                            if(tmp != null)
                                conns=tmp;
                        }
                        conns.add(this);
                    }
                    break;
                case UNSUBSCRIBE:
                    headers=frame.getHeaders();
                    destination=headers.get("destination");
                    if(destination != null) {
                        Set<Connection> conns=subscriptions.get(destination);
                        if(conns != null) {
                            if(conns.remove(this) && conns.isEmpty())
                                subscriptions.remove(destination);
                        }
                    }
                    break;
                case BEGIN:
                    break;
                case COMMIT:
                    break;
                case ABORT:
                    break;
                case ACK:
                    break;
                case DISCONNECT:
                    break;
                default:
                    log.error("Verb " + frame.getVerb() + " is not handled");
                    break;
            }
        }

        /**
         * Sends back a response. The keys_and_values vararg array needs to have an even number of elements
         * @param response
         * @param keys_and_values
         */
        private void writeResponse(ServerResponse response, String ... keys_and_values) {
            String tmp=response.name();
            try {
                out.write(tmp.getBytes());
                out.write('\n');

                for(int i=0; i < keys_and_values.length; i++) {
                    String key=keys_and_values[i];
                    String val=keys_and_values[++i];
                    out.write((key + ": " + val + "\n").getBytes());
                }
                out.flush();
            }
            catch(IOException ex) {
                log.error("failed writing response " + response, ex);
            }
        }


        private Frame readFrame(DataInputStream in) throws IOException {
            String tmp_verb=Util.readLine(in);
            if(tmp_verb == null)
                throw new EOFException("reading verb");
            if(tmp_verb.length() == 0)
                return null;

            ClientVerb verb;

            try {
                verb=ClientVerb.valueOf(tmp_verb);
            }
            catch(IllegalArgumentException illegal_ex) {
                log.error("verb " + tmp_verb + " unknown");
                return null;
            }

            Map<String,String> headers=new HashMap<String,String>();
            byte[] body=null;

            for(;;) {
                String header=Util.readLine(in);
                if(header == null)
                    throw new EOFException("reading header");
                if(header.length() == 0)
                    break;
                int index=header.indexOf(":");
                if(index != -1)
                    headers.put(header.substring(0, index).trim(), header.substring(index+1).trim());
            }

            if(headers.containsKey("length")) {
                int length=Integer.parseInt(headers.get("length"));
                body=new byte[length];
                in.read(body, 0, body.length);
            }
            else {
                ByteBuffer buf=ByteBuffer.allocate(500);
                boolean terminate=false;
                for(;;) {
                    int c=in.read();
                    if(c == -1 || c == 0)
                        terminate=true;

                    if(buf.remaining() == 0 || terminate) {
                        if(body == null) {
                            body=new byte[buf.position()];
                            System.arraycopy(buf.array(), buf.arrayOffset(), body, 0, buf.position());
                        }
                        else {
                            byte[] tmp=new byte[body.length + buf.position()];
                            System.arraycopy(body, 0, tmp, 0, body.length);
                            try {
                                System.arraycopy(buf.array(), buf.arrayOffset(), tmp, body.length, buf.position());
                            }
                            catch(Throwable t) {
                            }
                            body=tmp;
                        }
                        buf.rewind();
                    }

                    if(terminate)
                        break;

                    buf.put((byte)c);
                }
            }


            return new Frame(verb, headers, body);
        }
    }

    protected static class Frame {
        final ClientVerb verb;
        final Map<String,String> headers;
        final byte[] body;

        public Frame(ClientVerb verb, Map<String, String> headers, byte[] body) {
            this.verb=verb;
            this.headers=headers;
            this.body=body;
        }

        public byte[] getBody() {
            return body;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public ClientVerb getVerb() {
            return verb;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(verb).append("\n");
            if(headers != null && !headers.isEmpty()) {
                for(Map.Entry<String,String> entry: headers.entrySet())
                    sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            if(body != null && body.length > 0) {
                sb.append("body: ").append(body.length).append(" bytes");
                if(body.length < 50)
                    sb.append(": " + new String(body));
            }
            return sb.toString();
        }
    }
}
