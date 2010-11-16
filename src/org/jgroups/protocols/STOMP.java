package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Protocol which provides STOMP (http://stomp.codehaus.org/) support. Very simple implementation, with a
 * one-thread-per-connection model. Use for a few hundred clients max.<p/>
 * The intended use for this protocol is pub-sub with clients which handle text messages, e.g. stock updates,
 * SMS messages to mobile clients, SNMP traps etc.<p/>
 * Note that the full STOMP protocol has not yet been implemented, e.g. transactions are not supported.
 * todo: use a thread pool to handle incoming frames and to send messages to clients
 * <p/>
 * todo: add PING to test health of client connections
 * <p/> 
 * @author Bela Ban
 * @since 2.11
 */
@MBean(description="Server side STOPM protocol, STOMP clients can connect to it")
@Experimental
public class STOMP extends Protocol implements Runnable {

    /* -----------------------------------------    Properties     ----------------------------------------------- */

    @Property(name="bind_addr",
              description="The bind address which should be used by the server socket. The following special values " +
                      "are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              defaultValueIPv4="0.0.0.0", defaultValueIPv6="::",
              systemProperty={Global.STOMP_BIND_ADDR},writable=false)
    protected InetAddress bind_addr=null;

    @Property(description="If set, then endpoint will be set to this address",systemProperty=Global.STOMP_ENDPOINT_ADDR)
    protected String endpoint_addr;

    @Property(description="Port on which the STOMP protocol listens for requests",writable=false)
    protected int port=8787;

    @Property(description="If set to false, then a destination of /a/b match /a/b/c, a/b/d, a/b/c/d etc")
    protected boolean exact_destination_match=true;

    @Property(description="If true, information such as a list of endpoints, or views, will be sent to all clients " +
            "(via the INFO command). This allows for example intelligent clients to connect to " +
            "a different server should a connection be closed.")
    protected boolean send_info=true;

    @Property(description="Forward received messages which don't have a StompHeader to clients")
    protected boolean forward_non_client_generated_msgs=false;

    /* ---------------------------------------------   JMX      ---------------------------------------------------*/
    @ManagedAttribute(description="Number of client connections",writable=false)
    public int getNumConnections() {return connections.size();}

    @ManagedAttribute(description="Number of subscriptions",writable=false)
    public int getNumSubscriptions() {return subscriptions.size();}

    @ManagedAttribute(description="Print subscriptions",writable=false)
    public String getSubscriptions() {return subscriptions.keySet().toString();}

    @ManagedAttribute
    public String getEndpoints() {return endpoints.toString();}

    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected Address                   local_addr;
    protected ServerSocket              srv_sock;
    @ManagedAttribute(writable=false)
    protected String                    endpoint;
    protected Thread                    acceptor;
    protected final List<Connection>    connections=new LinkedList<Connection>();
    protected final Map<Address,String> endpoints=new HashMap<Address,String>();

    protected View view;

    // Subscriptions and connections which are subscribed
    protected final ConcurrentMap<String,Set<Connection>> subscriptions=Util.createConcurrentMap(20);

    public static enum ClientVerb      {CONNECT, SEND, SUBSCRIBE, UNSUBSCRIBE, BEGIN, COMMIT, ABORT, ACK, DISCONNECT}
    public static enum ServerVerb      {MESSAGE, RECEIPT, ERROR, CONNECTED, INFO}

    public static final byte           NULL_BYTE=0;


    
    public STOMP() {
    }



    public void start() throws Exception {
        super.start();
        srv_sock=Util.createServerSocket(getSocketFactory(), Global.STOMP_SRV_SOCK, bind_addr, port);
        if(log.isDebugEnabled())
            log.debug("server socket listening on " + srv_sock.getLocalSocketAddress());

        if(acceptor == null) {
            acceptor=getThreadFactory().newThread(this, "STOMP acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        endpoint=endpoint_addr != null? endpoint_addr : getAddress();
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
                conn.sendInfo();
            }
            catch(IOException io_ex) {
                break;
            }
        }
        acceptor=null;
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                StompHeader hdr=(StompHeader)msg.getHeader(id);
                if(hdr == null) {
                    if(forward_non_client_generated_msgs) {
                        HashMap<String, String> hdrs=new HashMap<String, String>();
                        hdrs.put("sender", msg.getSrc().toString());
                        sendToClients(hdrs, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    }
                    break;
                }

                switch(hdr.type) {
                    case MESSAGE:
                        sendToClients(hdr.headers, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        break;
                    case ENDPOINT:
                        String tmp_endpoint=hdr.headers.get("endpoint");
                        if(tmp_endpoint != null) {
                            boolean update_clients;
                            String old_endpoint=null;
                            synchronized(endpoints) {
                                endpoints.put(msg.getSrc(), tmp_endpoint);
                            }
                            update_clients=old_endpoint == null || !old_endpoint.equals(tmp_endpoint);
                            if(update_clients && this.send_info) {
                                synchronized(connections) {
                                    for(Connection conn: connections) {
                                        conn.writeResponse(ServerVerb.INFO, "endpoints", getAllEndpoints());
                                    }
                                }
                            }
                        }
                        return null;
                    default:
                        throw new IllegalArgumentException("type " + hdr.type + " is not known");
                }
                break;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        

        return up_prot.up(evt);
    }


    public static Frame readFrame(DataInputStream in) throws IOException {
        String verb=Util.readLine(in);
        if(verb == null)
            throw new EOFException("reading verb");
        if(verb.length() == 0)
            return null;
        verb=verb.trim();
        
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


    protected void handleView(View view) {
        broadcastEndpoint();
        List<Address> mbrs=view.getMembers();
        this.view=view;
        
        synchronized(endpoints) {
            endpoints.keySet().retainAll(mbrs);
        }

        synchronized(connections) {
            for(Connection conn: connections)
                conn.sendInfo();
        }
    }

    private String getAddress() {
        InetSocketAddress saddr=(InetSocketAddress)srv_sock.getLocalSocketAddress();
        InetAddress tmp=saddr.getAddress();
        if(!tmp.isAnyLocalAddress())
            return tmp.getHostAddress() + ":" + srv_sock.getLocalPort();

        for(Util.AddressScope scope: Util.AddressScope.values()) {
            try {
                InetAddress addr=Util.getAddress(scope);
                if(addr != null) return addr.getHostAddress() + ":" + srv_sock.getLocalPort();
            }
            catch(SocketException e) {
            }
        }
        return null;
    }

    protected String getAllEndpoints() {
        synchronized(endpoints) {
            return Util.printListWithDelimiter(endpoints.values(), ",");
        }
    }

//    protected String getAllClients() {
//        StringBuilder sb=new StringBuilder();
//        boolean first=true;
//
//        synchronized(connections) {
//            for(Connection conn: connections) {
//                UUID session_id=conn.session_id;
//                if(session_id != null) {
//                    if(first)
//                        first=false;
//                    else
//                        sb.append(",");
//                    sb.append(session_id);
//                }
//            }
//        }
//
//        return sb.toString();
//    }

    protected void broadcastEndpoint() {
        if(endpoint != null) {
            Message msg=new Message();
            msg.putHeader(id, StompHeader.createHeader(StompHeader.Type.ENDPOINT, "endpoint", endpoint));
            down_prot.down(new Event(Event.MSG, msg));
        }
    }

//    private void sendToClients(String destination, String sender, byte[] buffer, int offset, int length) {
//        int len=50 + length + (ServerVerb.MESSAGE.name().length() + 2)
//                + (destination != null? destination.length()+ 2 : 0)
//                + (sender != null? sender.length() +2 : 0)
//                + (buffer != null? 20 : 0);
//
//        ByteBuffer buf=ByteBuffer.allocate(len);
//
//        StringBuilder sb=new StringBuilder(ServerVerb.MESSAGE.name()).append("\n");
//        if(destination != null)
//            sb.append("destination: ").append(destination).append("\n");
//        if(sender != null)
//            sb.append("sender: ").append(sender).append("\n");
//        if(buffer != null)
//            sb.append("content-length: ").append(String.valueOf(length)).append("\n");
//        sb.append("\n");
//
//        byte[] tmp=sb.toString().getBytes();
//
//        if(buffer != null) {
//            buf.put(tmp, 0, tmp.length);
//            buf.put(buffer, offset, length);
//        }
//        buf.put(NULL_BYTE);
//
//        final Set<Connection> target_connections=new HashSet<Connection>();
//        if(destination == null) {
//            synchronized(connections) {
//                target_connections.addAll(connections);
//            }
//        }
//        else {
//            if(!exact_destination_match) {
//                for(Map.Entry<String,Set<Connection>> entry: subscriptions.entrySet()) {
//                    if(entry.getKey().startsWith(destination))
//                        target_connections.addAll(entry.getValue());
//                }
//            }
//            else {
//                Set<Connection> conns=subscriptions.get(destination);
//                if(conns != null)
//                    target_connections.addAll(conns);
//            }
//        }
//
//        for(Connection conn: target_connections)
//            conn.writeResponse(buf.array(), buf.arrayOffset(), buf.position());
//    }


    private void sendToClients(Map<String,String> headers, byte[] buffer, int offset, int length) {
        int len=50 + length + (ServerVerb.MESSAGE.name().length() + 2);
        if(headers != null) {
            for(Map.Entry<String,String> entry: headers.entrySet()) {
                len+=entry.getKey().length() +2;
                len+=entry.getValue().length() +2;
                len+=5; // fill chars, such as ": " or "\n"
            }
        }
        len+=(buffer != null? 20 : 0);
        ByteBuffer buf=ByteBuffer.allocate(len);

        StringBuilder sb=new StringBuilder(ServerVerb.MESSAGE.name()).append("\n");
        if(headers != null) {
            for(Map.Entry<String,String> entry: headers.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }

        if(buffer != null)
            sb.append("content-length: ").append(String.valueOf(length)).append("\n");
        sb.append("\n");

        byte[] tmp=sb.toString().getBytes();

        if(buffer != null) {
            buf.put(tmp, 0, tmp.length);
            buf.put(buffer, offset, length);
        }
        buf.put(NULL_BYTE);

        final Set<Connection> target_connections=new HashSet<Connection>();
        String destination=headers != null? headers.get("destination") : null;
        if(destination == null) {
            synchronized(connections) {
                target_connections.addAll(connections);
            }
        }
        else {
            if(!exact_destination_match) {
                for(Map.Entry<String,Set<Connection>> entry: subscriptions.entrySet()) {
                    if(entry.getKey().startsWith(destination))
                        target_connections.addAll(entry.getValue());
                }
            }
            else {
                Set<Connection> conns=subscriptions.get(destination);
                if(conns != null)
                    target_connections.addAll(conns);
            }
        }

        for(Connection conn: target_connections)
            conn.writeResponse(buf.array(), buf.arrayOffset(), buf.position());
    }


    /**
     * Class which handles a connection to a client
     */
    public class Connection implements Runnable {
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
            for(Set<Connection> conns: subscriptions.values()) {
                conns.remove(this);
            }
            for(Iterator<Map.Entry<String,Set<Connection>>> it=subscriptions.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String,Set<Connection>> entry=it.next();
                if(entry.getValue().isEmpty())
                    it.remove();
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
            ClientVerb verb=ClientVerb.valueOf(frame.getVerb());
            
            switch(verb) {
                case CONNECT:
                    writeResponse(ServerVerb.CONNECTED,
                                  "session-id", session_id.toString(),
                                  "password-check", "none");
                    break;
                case SEND:
                    if(!headers.containsKey("sender")) {
                        headers.put("sender", session_id.toString());
                    }

                    Message msg=new Message(null, null, frame.getBody());
                    Header hdr=StompHeader.createHeader(StompHeader.Type.MESSAGE, headers);
                    msg.putHeader(id, hdr);
                    down_prot.down(new Event(Event.MSG, msg));
                    String receipt=headers.get("receipt");
                    if(receipt != null)
                        writeResponse(ServerVerb.RECEIPT, "receipt-id", receipt);
                    break;
                case SUBSCRIBE:
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

        public void sendInfo() {
            if(send_info) {
                writeResponse(ServerVerb.INFO,
                              "local_addr", local_addr != null? local_addr.toString() : "n/a",
                              "view", view.toString(),
                              "endpoints", getAllEndpoints());
                              // "clients", getAllClients());
            }
        }

        /**
         * Sends back a response. The keys_and_values vararg array needs to have an even number of elements
         * @param response
         * @param keys_and_values
         */
        private void writeResponse(ServerVerb response, String ... keys_and_values) {
            String tmp=response.name();
            try {
                out.write(tmp.getBytes());
                out.write('\n');

                for(int i=0; i < keys_and_values.length; i++) {
                    String key=keys_and_values[i];
                    String val=keys_and_values[++i];
                    out.write((key + ": " + val + "\n").getBytes());
                }
                out.write("\n".getBytes());
                out.write(NULL_BYTE);
                out.flush();
            }
            catch(IOException ex) {
                log.error("failed writing response " + response + ": " + ex);
            }
        }

        private void writeResponse(byte[] response, int offset, int length) {
            try {
                out.write(response, offset, length);
                out.flush();
            }
            catch(IOException ex) {
                log.error("failed writing response: " + ex);
            }
        }
    }


    public static class Frame {
        final String             verb;
        final Map<String,String> headers;
        final byte[]             body;

        public Frame(String verb, Map<String, String> headers, byte[] body) {
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

        public String getVerb() {
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
                sb.append("body: ");
                if(body.length < 50)
                    sb.append(new String(body)).append(" (").append(body.length).append(" bytes)");
                else
                    sb.append(body.length).append(" bytes");
            }
            return sb.toString();
        }
    }


    public static class StompHeader extends org.jgroups.Header {
        public static enum Type {MESSAGE, ENDPOINT}

        protected Type                      type;
        protected final Map<String,String>  headers=new HashMap<String,String>();


        public StompHeader() {
        }

        private StompHeader(Type type) {
            this.type=type;
        }

        /**
         * Creates a new header
         * @param type
         * @param headers Keys and values to be added to the header hashmap. Needs to be an even number
         * @return
         */
        public static StompHeader createHeader(Type type, String ... headers) {
            StompHeader retval=new StompHeader(type);
            if(headers != null) {
                for(int i=0; i < headers.length; i++) {
                    String key=headers[i];
                    String value=headers[++i];
                    retval.headers.put(key, value);
                }
            }
            return retval;
        }

        public static StompHeader createHeader(Type type, Map<String,String> headers) {
            StompHeader retval=new StompHeader(type);
            if(headers != null)
                retval.headers.putAll(headers);
            return retval;
        }



        public int size() {
            int retval=Global.INT_SIZE *2; // type + size of hashmap
            for(Map.Entry<String,String> entry: headers.entrySet()) {
                retval+=entry.getKey().length() +2;
                retval+=entry.getValue().length() +2;
            }
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(type.ordinal());
            out.writeInt(headers.size());
            for(Map.Entry<String,String> entry: headers.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=Type.values()[in.readInt()];
            int size=in.readInt();
            for(int i=0; i < size; i++) {
                String key=in.readUTF();
                String value=in.readUTF();
                headers.put(key, value);
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(type.toString());
            sb.append("headers: ").append(headers);
            return sb.toString();
        }
    }
}
