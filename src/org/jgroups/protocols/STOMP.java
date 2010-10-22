package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
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
 * Protocol which provides STOMP (http://stomp.codehaus.org/) support. Very simple implementation, with a
 * one-thread-per-connection model. Use for a few hundred clients max.<p/>
 * The intended use for this protocol is pub-sub with clients which handle text messages, e.g. stock updates,
 * SMS messages to mobile clients, SNMP traps etc.
 * @author Bela Ban
 * @version $Id: STOMP.java,v 1.8 2010/10/22 16:28:58 belaban Exp $
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
    protected ServerSocket             srv_sock;
    protected Thread                   acceptor;
    protected final List<Connection>   connections=new LinkedList<Connection>();

    // Subscriptions and connections which are subscribed
    protected final ConcurrentMap<String, Set<Connection>> subscriptions=Util.createConcurrentMap(20);



    public static enum ClientVerb      {CONNECT, SEND, SUBSCRIBE, UNSUBSCRIBE, BEGIN, COMMIT, ABORT, ACK, DISCONNECT};
    public static enum ServerVerb      {MESSAGE, RECEIPT, ERROR}
    public static enum ServerResponse  {CONNECTED}

    public static final byte           NULL_BYTE=0;

    
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


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                StompHeader hdr=(StompHeader)msg.getHeader(id);
                String destination=hdr != null? hdr.destination : null;
                String sender=msg.getSrc() != null? msg.getSrc().toString() : "n/a";
                sendToClients(destination, sender, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                break;
        }
        

        return up_prot.up(evt);
    }

    private void sendToClients(String destination, String sender, byte[] buffer, int offset, int length) {
        int len=50 + length + (ServerVerb.MESSAGE.name().length() + 2) 
                + (destination != null? destination.length()+ 2 : 0)
                + (sender != null? sender.length() +2 : 0)
                + (buffer != null? 20 : 0);

        ByteBuffer buf=ByteBuffer.allocate(len);

        StringBuilder sb=new StringBuilder(ServerVerb.MESSAGE.name()).append("\n");
        if(destination != null)
            sb.append("destination: ").append(destination).append("\n");
        if(sender != null)
            sb.append("sender: ").append(sender).append("\n");
        if(buffer != null)
            sb.append("content-length: ").append(String.valueOf(length)).append("\n");
        sb.append("\n");

        byte[] tmp=sb.toString().getBytes();

        if(buffer != null) {
            buf.put(tmp, 0, tmp.length);
            buf.put(buffer, offset, length);
        }
        buf.put(NULL_BYTE);

        final List<Connection> target_connections=new ArrayList<Connection>();
        if(destination == null) {
            synchronized(connections) {
                target_connections.addAll(connections);
            }
        }
        else {
            Set<Connection> conns=subscriptions.get(destination);
            if(conns != null)
                target_connections.addAll(conns);
        }

        for(Connection conn: target_connections)
            conn.writeResponse(buf.array(), buf.arrayOffset(), buf.position());
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
                catch(IOException ex) {
                    stop();
                    remove();
                }
                catch(Throwable t) {
                    log.error("failure reading frame", t);
                }
            }
        }


        protected void handleFrame(Frame frame) {
            Map<String,String> headers=frame.getHeaders();
            switch(frame.getVerb()) {
                case CONNECT:
                    writeResponse(ServerResponse.CONNECTED,
                                  "session-id", session_id.toString(),
                                  "password-check", "none");
                    break;
                case SEND:
                    String destination=headers.get("destination");
                    String sender=session_id.toString();

                    Message msg=new Message(null, null, frame.getBody());
                    Header hdr=new StompHeader(destination);
                    msg.putHeader(id, hdr);
                    down_prot.down(new Event(Event.MSG, msg));
                    break;
                case SUBSCRIBE:
                    destination=headers.get("destination");
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

        private void writeResponse(byte[] response, int offset, int length) {
            try {
                out.write(response, offset, length);
                out.flush();
            }
            catch(IOException ex) {
                log.error("failed writing response", ex);
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

            if(headers.containsKey("content-length")) {
                int length=Integer.parseInt(headers.get("content-length"));
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


    public static class StompHeader extends org.jgroups.Header {
        protected String destination;

        public StompHeader() {
        }

        public StompHeader(String destination) {
            this.destination=destination;
        }

        public int size() {
            return Global.BYTE_SIZE // presence
                    + (destination != null? destination.length() +2 : 0);
        }

        public void writeTo(DataOutputStream out) throws IOException {
            Util.writeString(destination, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            destination=Util.readString(in);
        }
    }
}
