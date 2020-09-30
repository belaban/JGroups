package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@code STATE_SOCK} has the state provider create a server socket to which the state
 * requester connects and from which the latter reads the state.
 * <p/>
 * When implementing {@link org.jgroups.Receiver#getState(java.io.OutputStream)}, the state should be written in
 * sizeable chunks, because the underlying output stream sends 1 message / write over the socket. So if there are 1000
 * writes of 1 byte each, this would generate 1000 messages ! We suggest using a {@link java.io.BufferedOutputStream}
 * over the output stream handed to the application as argument of the callback.
 * <p/>
 * When implementing the {@link org.jgroups.Receiver#setState(java.io.InputStream)} callback, there is no need to use a
 * {@link java.io.BufferedOutputStream}, as the input stream handed to the application already buffers incoming data
 * internally.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * @see STATE_TRANSFER
 * @since 3.0
 */
@MBean(description="State trasnfer protocol based on streaming state transfer")
public class STATE_SOCK extends StreamingStateTransfer {

    /*
     * ----------------------------------------------Properties -----------------------------------
     */
    @LocalAddress
    @Property(description="The interface (NIC) used to accept state requests. " +
      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress bind_addr;

    @Property(description="Use \"external_addr\" if you have hosts on different networks, behind " +
      "firewalls. On each firewall, set up a port forwarding rule (sometimes called \"virtual server\") to " +
      "the local IP (e.g. 192.168.1.100) of the host then on each host, set \"external_addr\" TCP transport " +
      "parameter to the external (public IP) address of the firewall.",
              systemProperty=Global.EXTERNAL_ADDR,writable=false)
    protected InetAddress external_addr;

    @Property(description="Used to map the internal port (bind_port) to an external port. Only used if > 0",
              systemProperty=Global.EXTERNAL_PORT,writable=false)
    protected int external_port;
    
    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class,
              description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr")
    protected String bind_interface_str;

    @Property(description="The port listening for state requests. Default value of 0 binds to any (ephemeral) port")
    protected int bind_port;

    @Property(description="The size (in bytes) of the receive buffer of the socket",type=AttributeType.BYTES)
    protected int recv_buf_size;

    /*
    * --------------------------------------------- Fields ---------------------------------------
    */

    /**
     * Runnable that listens for state requests and spawns threads to serve those requests if socket transport is used
     */
    protected volatile StateProviderAcceptor spawner;


    public STATE_SOCK() {
        super();
    }


    public void stop() {
        super.stop();
        if(spawner != null)
            spawner.stop();
    }


    /*
    * --------------------------- Private Methods ------------------------------------------------
    */

    protected StateProviderAcceptor createAcceptor() {
        StateProviderAcceptor retval=new StateProviderAcceptor(thread_pool,
                                                               Util.createServerSocket(getSocketFactory(),
                                                                                       "jgroups.streaming_state_transfer.srv_sock",
                                                                                       bind_addr, bind_port, bind_port, recv_buf_size));
        Thread t=getThreadFactory().newThread(retval, "STATE server socket acceptor");
        t.start();
        return retval;
    }


    protected void modifyStateResponseHeader(StateHeader hdr) {
        if(spawner != null)
            hdr.bind_addr=spawner.getServerSocketAddress();
    }


    protected Tuple<InputStream,Object> createStreamToProvider(Address provider, StateHeader hdr) throws Exception {
        IpAddress address=hdr.bind_addr;
        Socket socket=null;
        try {
            socket=getSocketFactory().createSocket("jgroups.state_sock.sock");
            socket.bind(new InetSocketAddress(bind_addr, 0));
            socket.setReceiveBufferSize(buffer_size);
            Util.connect(socket, new InetSocketAddress(address.getIpAddress(), address.getPort()), 0);
            log.debug("%s: connected to state provider %s:%d", local_addr, address.getIpAddress(), address.getPort());
            DataOutputStream out=new DataOutputStream(socket.getOutputStream());
            Util.writeAddress(local_addr, out);
            return new Tuple<>(new BufferedInputStream(socket.getInputStream(), buffer_size), socket);
        }
        catch(Throwable t) {
            Util.close(socket);
            if(t instanceof Exception)
                throw (Exception)t;
            throw new Exception("failed creating socket", t);
        }
    }

    protected void close(Object resource) {
        if(resource instanceof Socket)
            Util.close((Socket)resource);
    }

    protected void handleStateReq(Address requester) {
        if(spawner == null || !spawner.isRunning())
            spawner=createAcceptor();
        super.handleStateReq(requester);
    }


    protected void handleViewChange(View v) {
        super.handleViewChange(v);
        if(state_provider != null && !v.getMembers().contains(state_provider)) {
            openBarrierAndResumeStable();
            Exception ex=new EOFException("state provider " + state_provider + " left");
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult(ex)));
        }
    }

    protected void handleConfig(Map<String,Object> config) {
        super.handleConfig(config);
        if(bind_addr == null)
            bind_addr=(InetAddress)config.get("bind_addr");
        if(external_addr == null)
            external_addr=(InetAddress)config.get("external_addr");
        if(external_port <= 0) {
            Object val=config.get("external_port");
            if(val != null)
                external_port=(Integer)val;
        }
    }


    /*
    * ------------------------ End of Private Methods --------------------------------------------
    */

    protected class StateProviderAcceptor implements Runnable {
        protected final ExecutorService pool;
        protected final ServerSocket    serverSocket;
        protected final IpAddress       address;
        protected volatile boolean      running=true;

        public StateProviderAcceptor(ExecutorService pool, ServerSocket stateServingSocket) {
            super();
            this.pool=pool;
            this.serverSocket=stateServingSocket;
            if(external_addr != null)
                this.address=new IpAddress(external_addr, external_port > 0? external_port : serverSocket.getLocalPort());
            else
                this.address=new IpAddress(bind_addr, serverSocket.getLocalPort());
        }

        public IpAddress getServerSocketAddress() {return address;}
        public boolean   isRunning()              {return running;}

        public void run() {
            if(log.isDebugEnabled())
                log.debug(local_addr + ": StateProviderAcceptor listening at " + getServerSocketAddress());
            while(running) {
                try {
                    final Socket socket=serverSocket.accept();
                    try {
                        pool.execute(() -> process(socket));
                    }
                    catch(RejectedExecutionException rejected) {
                        Util.close(socket);
                    }
                }
                catch(Throwable e) {
                    if(serverSocket.isClosed())
                        running=false;
                }
            }
        }

        protected void process(Socket socket) {
            OutputStream      output=null;
            try {
                socket.setSendBufferSize(buffer_size);
                if(log.isDebugEnabled())
                    log.debug(local_addr + ": accepted request for state transfer from " + socket.getInetAddress() + ":" + socket.getPort());

                DataInput in=new DataInputStream(socket.getInputStream());
                Address stateRequester=Util.readAddress(in);
                output=new BufferedOutputStream(socket.getOutputStream(), buffer_size);
                getStateFromApplication(stateRequester, output, false);
            }
            catch(Throwable e) {
                if(log.isWarnEnabled())
                    log.warn(local_addr + ": failed handling request from requester", e);
            }
            // getStateFromApplication() is run in the same thread; it closes the output stream, and we close the socket
            finally {
                Util.close(socket);
            }
        }



        public void stop() {
            running=false;
            try {
                getSocketFactory().close(serverSocket);
            }
            catch(Exception ignored) {
            }
        }
    }


}
