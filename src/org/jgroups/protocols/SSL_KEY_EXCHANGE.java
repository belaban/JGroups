package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Runner;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.*;
import java.io.*;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.Map;
import java.util.Objects;

/**
 * Key exchange based on SSL sockets. The key server creates an {@link javax.net.ssl.SSLServerSocket} on a given port
 * and members fetch the secret key by creating a {@link javax.net.ssl.SSLSocket} to the key server. The key server
 * authenticates the client (and vice versa) and then sends the secret key over this encrypted channel.
 * <br/>
 * When the key exchange has completed, the secret key requester closes its SSL connection to the key server.
 * <br/>
 * Note that this implementation should prevent man-in-the-middle attacks.
 * @author Bela Ban
 * @since  4.0.5
 */
@SuppressWarnings("unused")
@MBean(description="Key exchange protocol based on an SSL connection between secret key requester and provider " +
  "(key server) to fetch a shared secret group key from the key server. That shared (symmetric) key is subsequently " +
  "used to encrypt communication between cluster members")
public class SSL_KEY_EXCHANGE extends KeyExchange {

    protected enum Type {
        SECRET_KEY_REQ,
        SECRET_KEY_RSP // data: | length | version | length | secret key |
    }

    public interface SessionVerifier {

        /** Called after creation with session_verifier_arg */
        void init(String arg);

        /**
         * Called to verify that the session is correct, e.g. by matching the peer's certificate-CN. Needs to throw a
         * SecurityException if not
         */
        void verify(SSLSession session) throws SecurityException;
    }

    @Property(description="The port at which the key server is listening. If the port is not available, the next port " +
      "will be probed, up to port+port_range. Used by the key server (server) to create an SSLServerSocket and " +
      "by clients to connect to the key server.")
    protected int             port=2157;

    @LocalAddress
    @Property(description="Bind address for the server or client socket. " +
      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
      systemProperty={Global.BIND_ADDR})
    protected InetAddress     bind_addr;

    @Property(description="The port range to probe")
    protected int             port_range=5;

    @Property(description="Location of the keystore")
    protected String          keystore_name="keystore.jks";

    @Property(description="The type of the keystore. " +
      "Types are listed in http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html")
    protected String          keystore_type="JKS";

    @Property(description="Password to access the keystore",exposeAsManagedAttribute=false)
    protected String          keystore_password="changeit";

    @Property(description="The type of secret key to be sent up the stack (converted from DH). " +
      "Should be the same as ASYM_ENCRYPT.sym_algorithm if ASYM_ENCRYPT is used")
    protected String          secret_key_algorithm="AES";

    @Property(description="If enabled, clients are authenticated as well (not just the server). Set to true to prevent " +
      "rogue clients to fetch the secret group key (e.g. via man-in-the-middle attacks)")
    protected boolean         require_client_authentication=true;

    @Property(description="Timeout (in ms) for a socket read. This applies for example to the initial SSL handshake, " +
      "e.g. if the client connects to a non-JGroups service accidentally running on the same port")
    protected int             socket_timeout=1000;

    @Property(description="The fully qualified name of a class implementing SessionVerifier")
    protected String          session_verifier_class;

    @Property(description="The argument to the session verifier")
    protected String          session_verifier_arg;


    protected SSLServerSocket srv_sock;

    protected Runner          srv_sock_handler;

    protected KeyStore        key_store;

    protected View            view;

    protected SessionVerifier session_verifier;


    public void init() throws Exception {
        super.init();
        if(port == 0)
            throw new IllegalStateException("port must not be 0 or else clients will not be able to connect");
        ASYM_ENCRYPT asym_encrypt=findProtocolAbove(ASYM_ENCRYPT.class);
        if(asym_encrypt != null) {
            String sym_alg=asym_encrypt.symAlgorithm();
            if(!Util.match(sym_alg, secret_key_algorithm)) {
                log.warn("overriding %s=%s to %s from %s", "secret_key_algorithm", secret_key_algorithm,
                         sym_alg, ASYM_ENCRYPT.class.getSimpleName());
                secret_key_algorithm=sym_alg;
            }
        }
        key_store=KeyStore.getInstance(keystore_type != null? keystore_type : KeyStore.getDefaultType());
        key_store.load(new FileInputStream(keystore_name), keystore_password.toCharArray());
        if(session_verifier_class != null) {
            Class<? extends SessionVerifier> verifier_class=Util.loadClass(session_verifier_class, getClass());
            session_verifier=verifier_class.newInstance();
            if(session_verifier_arg != null)
                session_verifier.init(session_verifier_arg);
        }
    }

    public void start() throws Exception {
        super.start();
    }

    public void stop() {
        super.stop();
        if(srv_sock_handler != null) {
            srv_sock_handler.stop(); // should also close srv_sock
            srv_sock_handler=null;
            Util.close(srv_sock);  // not needed, but second line of defense
            srv_sock=null;
        }
    }

    public void destroy() {
        super.destroy();
    }

    @SuppressWarnings("unchecked")
    public Object up(Event evt) {
        if(evt.getType() == Event.CONFIG) {
            if(bind_addr == null) {
                Map<String,Object> config=evt.getArg();
                bind_addr=(InetAddress)config.get("bind_addr");
            }
            return up_prot.up(evt);
        }
        return super.up(evt);
    }


    public synchronized void fetchSecretKeyFrom(Address target) throws Exception {
        try(SSLSocket sock=createSocketTo(target)) {
            DataInput in=new DataInputStream(sock.getInputStream());
            OutputStream out=sock.getOutputStream();

            // send the secret key request
            out.write(Type.SECRET_KEY_REQ.ordinal());
            out.flush();

            byte ordinal=in.readByte();
            Type rsp=Type.values()[ordinal];
            if(rsp != Type.SECRET_KEY_RSP)
                throw new IllegalStateException(String.format("expected response of %s but got type=%d", Type.SECRET_KEY_RSP, ordinal));

            int version_len=in.readInt();
            byte[] version=new byte[version_len];
            in.readFully(version);

            int secret_key_len=in.readInt();
            byte[] secret_key=new byte[secret_key_len];
            in.readFully(secret_key);

            SecretKey sk=new SecretKeySpec(secret_key, secret_key_algorithm);
            Tuple<SecretKey,byte[]> tuple=new Tuple<>(sk, version);
            log.debug("%s: sending up secret key (version: %s)", local_addr, Util.byteArrayToHexString(version));
            up_prot.up(new Event(Event.SET_SECRET_KEY, tuple));
        }
    }

    protected void handleView(View view) {
        Address old_coord=this.view != null? this.view.getCoord() : null;
        this.view=view;

        if(Objects.equals(view.getCoord(), local_addr)) {
            if(!Objects.equals(old_coord, local_addr)) {
                try {
                    becomeKeyserver();
                }
                catch(Exception e) {
                    log.error("failed becoming key server", e);
                }
            }
        }
        else { // stop being keyserver, close the server socket and handler
            if(Objects.equals(old_coord, local_addr))
                stopKeyserver();
        }
    }



    protected void accept() {
        try(SSLSocket client_sock=(SSLSocket)srv_sock.accept()) {
            client_sock.setEnabledCipherSuites(client_sock.getSupportedCipherSuites());
            client_sock.startHandshake();
            SSLSession sslSession=client_sock.getSession();

            log.debug("%s: accepted SSL connection from %s; protocol: %s, cipher suite: %s",
                      local_addr, client_sock.getRemoteSocketAddress(), sslSession.getProtocol(), sslSession.getCipherSuite());

            if(session_verifier != null)
                session_verifier.verify(sslSession);

            // Start handling application content
            InputStream in=client_sock.getInputStream();
            DataOutput out=new DataOutputStream(client_sock.getOutputStream());

            byte ordinal=(byte)in.read();
            Type req=Type.values()[ordinal];
            if(req != Type.SECRET_KEY_REQ)
                throw new IllegalStateException(String.format("expected request of %s but got type=%d", Type.SECRET_KEY_REQ, ordinal));

            Tuple<SecretKey,byte[]> tuple=(Tuple<SecretKey,byte[]>)up_prot.up(new Event(Event.GET_SECRET_KEY));
            if(tuple == null)
                return;
            byte[] version=tuple.getVal2();
            byte[] secret_key=tuple.getVal1().getEncoded();

            out.write(Type.SECRET_KEY_RSP.ordinal());
            out.writeInt(version.length);
            out.write(version, 0, version.length);
            out.writeInt(secret_key.length);
            out.write(secret_key);
        }
        catch(Throwable t) {
            log.warn("failure handling client socket", t);
        }
    }


    protected synchronized void becomeKeyserver() throws Exception {
        if(srv_sock == null || srv_sock.isClosed()) {
            log.debug("%s: becoming keyserver; creating server socket", local_addr);
            srv_sock=createServerSocket();
            srv_sock_handler=new Runner(getThreadFactory(), SSL_KEY_EXCHANGE.class.getSimpleName() + "-runner",
                                        this::accept, () -> Util.close(srv_sock));
            srv_sock_handler.start();
            log.debug("SSL server socket listening on %s", srv_sock.getLocalSocketAddress());
        }
    }

    protected synchronized void stopKeyserver() {
        if(srv_sock_handler != null) {
            log.debug("%s: ceasing to be the keyserver; closing the server socket", local_addr);
            srv_sock_handler.stop();
            srv_sock_handler=null;
        }
        if(srv_sock != null) {
            Util.close(srv_sock); // should not be necessary (check)
            srv_sock=null;
        }
    }


    protected SSLServerSocket createServerSocket() throws Exception {
        SSLContext ctx=getContext();
        SSLServerSocketFactory sslServerSocketFactory=ctx.getServerSocketFactory();

        SSLServerSocket sslServerSocket=null;
        for(int i=0; i < port_range; i++) {
            try {
                sslServerSocket=(SSLServerSocket)sslServerSocketFactory.createServerSocket(port + i, 50, bind_addr);
                sslServerSocket.setNeedClientAuth(require_client_authentication);
                return sslServerSocket;
            }
            catch(Throwable t) {
            }
        }
        throw new IllegalStateException(String.format("found no valid port to bind to in range [%d-%d]", port, port+port_range));
    }

    protected SSLSocket createSocketTo(Address target) throws Exception {
        SSLContext ctx=getContext();
        SSLSocketFactory sslSocketFactory=ctx.getSocketFactory();

        IpAddress dest=(IpAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, target));
        SSLSocket sock=null;
        for(int i=0; i < port_range; i++) {
            try {
                sock=(SSLSocket)sslSocketFactory.createSocket(dest.getIpAddress(), port+i);
                sock.setSoTimeout(socket_timeout);
                sock.setEnabledCipherSuites(sock.getSupportedCipherSuites());
                sock.startHandshake();
                SSLSession sslSession=sock.getSession();

                log.debug("%s: created SSL connection to %s (%s); protocol: %s, cipher suite: %s",
                          local_addr, target, sock.getRemoteSocketAddress(), sslSession.getProtocol(), sslSession.getCipherSuite());

                if(session_verifier != null)
                    session_verifier.verify(sslSession);
                return sock;
            }
            catch(SecurityException sec_ex) {
                throw sec_ex;
            }
            catch(Throwable t) {
            }
        }
        throw new IllegalStateException(String.format("failed connecting to any targets in range %s[%d - %d]",
                                                      dest.getIpAddress(), port, port+port_range));
    }




    protected SSLContext getContext() throws Exception {
        // Create key manager
        KeyManagerFactory keyManagerFactory=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(key_store, keystore_password.toCharArray());
        KeyManager[] km=keyManagerFactory.getKeyManagers();

        // Create trust manager
        // TrustManagerFactory trustManagerFactory=TrustManagerFactory.getInstance("SunX509");
        TrustManagerFactory trustManagerFactory=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(key_store);
        TrustManager[] tm=trustManagerFactory.getTrustManagers();

        // Initialize SSLContext
        SSLContext sslContext=SSLContext.getInstance("TLSv1");
        sslContext.init(km, tm, null);
        return sslContext;
    }


    protected static String print16(PublicKey pub_key) {
        // use SHA256 to create a hash of secret_key and only then truncate it to secret_key_length
        MessageDigest digest=null;
        try {
            digest=MessageDigest.getInstance("SHA-256");
            digest.update(pub_key.getEncoded());
            return Util.byteArrayToHexString(digest.digest(), 0, 16);
        }
        catch(NoSuchAlgorithmException e) {
            return e.toString();
        }
    }



}

