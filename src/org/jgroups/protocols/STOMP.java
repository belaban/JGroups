package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 * Protocol which provides STOMP support. Very simple implementation, with a 1 thread / connection model. Use for
 * a few hundred clients max.
 * @author Bela Ban
 * @version $Id: STOMP.java,v 1.1 2010/10/21 10:45:53 belaban Exp $
 * @since 2.11
 */
@MBean
@Experimental @Unsupported
public class STOMP extends Protocol implements Runnable {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    @Property(description="Port on which the STOMP protocol listens for requests",writable=false)
    protected int port=8787;



    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected ServerSocket           srv_sock;
    protected Thread                 acceptor;
    protected final List<Connection> connections=new LinkedList<Connection>();




    
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
            for(Connection conn: connections) {
                conn.stop();
            }
            connections.clear();
        }
        acceptor=null;
        super.stop();
    }

    // acceptor loop

    public void run() {
        Socket client_sock;
        while(acceptor != null && srv_sock != null) {
            try {
                if(log.isTraceEnabled()) // +++ remove
                    log.trace("waiting for client connections on " + srv_sock.getInetAddress() + ":" +
                            srv_sock.getLocalPort());
                client_sock=srv_sock.accept();
                if(log.isTraceEnabled()) // +++ remove
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

        public Connection(Socket sock) throws IOException {
            this.sock=sock;
            this.in=new DataInputStream(sock.getInputStream());
            this.out=new DataOutputStream(sock.getOutputStream());
        }

        public void stop() {
            Util.close(in);
            Util.close(out);
            Util.close(sock);
        }


        public void run() {
        }
    }
}
