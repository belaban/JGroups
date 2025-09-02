package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Runner;
import org.jgroups.util.SslContextFactory;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.*;
import java.io.*;
import java.net.InetAddress;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Key exchange based on SSL sockets. The key server creates an {@link javax.net.ssl.SSLServerSocket} on a given port
 * and members fetch the secret key by creating a {@link javax.net.ssl.SSLSocket} to the key server. The key server
 * authenticates the client (and vice versa) and then sends the secret key over this encrypted channel.
 * <p>
 * When the key exchange has completed, the secret key requester closes its SSL connection to the key server.
 * <p>
 * Note that this implementation should prevent man-in-the-middle attacks.
 * @author Bela Ban
 * @since  4.0.5
 */
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

    /**
     * Helper class that knows how and when to reload the SSLContext.
     */
    protected class SSLContextReloader {

        private SSLContext ssl_ctx;
        private SslContextFactory factory;

        private Duration reloadThreshold;
        private Instant lastReloadTime;

        public SSLContextReloader(long d) {
                reloadThreshold = Duration.ofMillis(d);
        }

        public void reload() {
            reload(false);
        }

        public void reload(boolean forceReload) {
            // If we don't have a factory it means that the SSLContext was
            // provided from outside the SSL_KEY_EXCHANGE class. In that case,
            // responsibility of reloading the SSLContext resides elsewhere.
            // If factory exists, the SSLContext was created by that factory,
            // thus it can be reloaded.
            if (factory == null) {
                return;
            }

            Instant now = Instant.now();
            if (!forceReload) {
                if (Duration.between(lastReloadTime, now).compareTo(reloadThreshold) < 0) {
                    return;
                }
            }

            factory.initializeContext(ssl_ctx);
            lastReloadTime = now;
        }

        public SSLContextReloader setContext(SSLContext ssl_ctx) {
            this.ssl_ctx = ssl_ctx;
            lastReloadTime = Instant.now();
            return this;
        }

        public SSLContextReloader setFactory(SslContextFactory factory) {
            this.factory = factory;
            return this;
        }
    }


    @LocalAddress
    @Property(description="Bind address for the server or client socket. " +
      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
      systemProperty={Global.BIND_ADDR})
    protected InetAddress     bind_addr;

    @Property(description="The port at which the key server is listening. If the port is not available, the next port " +
      "will be probed, up to port+port_range. Used by the key server (server) to create an SSLServerSocket and " +
      "by clients to connect to the key server.")
    protected int             port=2157;

    @Property(description="The port range to probe")
    protected int             port_range=5;

    @Property(description="Location of the keystore")
    protected String          keystore_name="keystore.jks";

    @Property(description="The type of the keystore. " +
      "Types are listed in http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html")
    protected String          keystore_type="pkcs12";

    @Property(description="Password to access the keystore",exposeAsManagedAttribute=false)
    protected String          keystore_password="changeit";

    @Property(description="Location of the truststore. Defaults to null, which will use the keystore as a truststore.")
    protected String          truststore_name;

    @Property(description="The type of the truststore. " +
      "Types are listed in http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html")
    protected String          truststore_type="PKCS12";

    @Property(description="Password to access the truststore",exposeAsManagedAttribute=false)
    protected String          truststore_password="changeit";

    @Property(description="Minimum time (in ms) before reloading the keystore and truststore from disk.")
    protected long            reload_threshold=60000;

    @Property(description="The type of secret key to be sent up the stack (converted from DH). " +
      "Should be the same as the algorithm part of ASYM_ENCRYPT.sym_algorithm if AS" +
      "YM_ENCRYPT is used")
    protected String          secret_key_algorithm="AES";

    @Property(description="If enabled, clients are authenticated as well (not just the server). Set to true to prevent " +
      "rogue clients to fetch the secret group key (e.g. via man-in-the-middle attacks)")
    protected boolean         require_client_authentication=true;

    @Property(description="The SSL protocol to use. Defaults to TLS")
    protected String          ssl_protocol=SslContextFactory.DEFAULT_SSL_PROTOCOL;

    @Property(description="The SSL security provider. Defaults to null, which will use the default JDK provider.")
    protected String          ssl_provider;

    @Property(description="Timeout (in ms) for a socket read. This applies for example to the initial SSL handshake, " +
      "e.g. if the client connects to a non-JGroups service accidentally running on the same port",type=AttributeType.TIME)
    protected int             socket_timeout=1000;

    @Property(description="The fully qualified name of a class implementing SessionVerifier")
    protected String          session_verifier_class;

    @Property(description="The argument to the session verifier")
    protected String          session_verifier_arg;

    protected SSLContext                   client_ssl_ctx;
    protected SSLContext                   server_ssl_ctx;
    protected SSLContextReloader           client_ssl_ctx_reloader;
    protected SSLContextReloader           server_ssl_ctx_reloader;
    protected SSLServerSocket              srv_sock;
    protected Runner                       srv_sock_handler;
    protected KeyStore                     key_store;
    protected KeyStore                     trust_store;
    protected View                         view;
    protected SessionVerifier              session_verifier;


    public InetAddress      getBindAddress()                               {return bind_addr;}
    public SSL_KEY_EXCHANGE setBindAddress(InetAddress a)                  {this.bind_addr=a; return this;}
    public int              getPort()                                      {return port;}
    public SSL_KEY_EXCHANGE setPort(int p)                                 {this.port=p; return this;}
    public int              getPortRange()                                 {return port_range;}
    public SSL_KEY_EXCHANGE setPortRange(int r)                            {this.port_range=r; return this;}
    public String           getKeystoreName()                              {return keystore_name;}
    public SSL_KEY_EXCHANGE setKeystoreName(String name)                   {this.keystore_name=name; return this;}
    public String           getKeystoreType()                              {return keystore_type;}
    public SSL_KEY_EXCHANGE setKeystoreType(String type)                   {this.keystore_type=type; return this;}
    public String           getKeystorePassword()                          {return keystore_password;}
    public SSL_KEY_EXCHANGE setKeystorePassword(String pwd)                {this.keystore_password=pwd; return this;}
    public String           getTruststoreName()                            {return truststore_name;}
    public SSL_KEY_EXCHANGE setTruststoreName(String name)                 {this.truststore_name=name; return this;}
    public String           getTruststoreType()                            {return truststore_type;}
    public SSL_KEY_EXCHANGE setTruststoreType(String type)                 {this.truststore_type=type; return this;}
    public String           getTruststorePassword()                        {return truststore_password;}
    public SSL_KEY_EXCHANGE setTruststorePassword(String pwd)              {this.truststore_password=pwd; return this;}
    public String           getSecretKeyAlgorithm()                        {return secret_key_algorithm;}
    public SSL_KEY_EXCHANGE setSecretKeyAlgorithm(String a)                {this.secret_key_algorithm=a; return this;}
    public boolean          getRequireClientAuthentication()               {return require_client_authentication;}
    public SSL_KEY_EXCHANGE setRequireClientAuthentication(boolean b)      {this.require_client_authentication=b; return this;}
    public SSL_KEY_EXCHANGE setSslProtocol(String protocol)                {this.ssl_protocol=protocol; return this;}
    public int              getSocketTimeout()                             {return socket_timeout;}
    public SSL_KEY_EXCHANGE setSocketTimeout(int timeout)                  {this.socket_timeout=timeout; return this;}
    public String           getSessionVerifierClass()                      {return session_verifier_class;}
    public SSL_KEY_EXCHANGE setSessionVerifierClass(String cl)             {this.session_verifier_class=cl; return this;}
    public String           getSessionVerifierArg()                        {return session_verifier_arg;}
    public SSL_KEY_EXCHANGE setSessionVerifierArg(String arg)              {this.session_verifier_arg=arg; return this;}
    public KeyStore         getKeystore()                                  {return key_store;}
    public SSL_KEY_EXCHANGE setKeystore(KeyStore ks)                       {this.key_store=ks; return this;}
    public KeyStore         getTruststore()                                {return trust_store;}
    public SSL_KEY_EXCHANGE setTruststore(KeyStore ks)                     {this.trust_store=ks; return this;}
    public SessionVerifier  getSessionVerifier()                           {return session_verifier;}
    public SSL_KEY_EXCHANGE setSessionVerifier(SessionVerifier s)          {this.session_verifier=s; return this;}
    public SSLContext       getClientSSLContext()                          {return client_ssl_ctx;}
    public SSL_KEY_EXCHANGE setClientSSLContext(SSLContext client_ssl_ctx) {this.client_ssl_ctx = client_ssl_ctx; return this;}
    public SSLContext       getServerSSLContext()                          {return server_ssl_ctx;}
    public SSL_KEY_EXCHANGE setServerSSLContext(SSLContext server_ssl_ctx) {this.server_ssl_ctx = server_ssl_ctx; return this;}
    public SSL_KEY_EXCHANGE setReloadThreshold(long d)                     {this.reload_threshold = d; return this;}

    public Address getServerLocation() {
        return srv_sock == null? null : new IpAddress(getTransport().getBindAddress(), srv_sock.getLocalPort());
    }

    public void init() throws Exception {
        super.init();
        if(port == 0)
            throw new IllegalStateException("port must not be 0 or else clients will not be able to connect");
        ASYM_ENCRYPT asym_encrypt=findProtocolAbove(ASYM_ENCRYPT.class);
        if(asym_encrypt != null) {
            String sym_alg=asym_encrypt.symKeyAlgorithm();
            if(!Util.match(sym_alg, secret_key_algorithm)) {
                log.warn("%s: overriding %s=%s to %s from %s", "secret_key_algorithm", local_addr, secret_key_algorithm,
                         sym_alg, ASYM_ENCRYPT.class.getSimpleName());
                secret_key_algorithm=sym_alg;
            }
        }

        if (truststore_name == null) {
            // Truststore not given, so use the keystore as a truststore for backwards compatibility
            truststore_name = keystore_name;
            truststore_type = keystore_type;
            truststore_password = keystore_password;
        }

        client_ssl_ctx_reloader = new SSLContextReloader(reload_threshold).setContext(client_ssl_ctx);
        server_ssl_ctx_reloader = new SSLContextReloader(reload_threshold).setContext(server_ssl_ctx);

        // Create an SSLContext if one was not already supplied
        if (client_ssl_ctx == null || server_ssl_ctx == null) {
            SslContextFactory sslContextFactory = new SslContextFactory();
            SSLContext sslContext = sslContextFactory
                    .classLoader(this.getClass().getClassLoader())
                    .keyStore(key_store)
                    .keyStoreType(keystore_type)
                    .keyStoreFileName(keystore_name)
                    .keyStorePassword(keystore_password.toCharArray())
                    .trustStore(trust_store)
                    .trustStoreType(truststore_type)
                    .trustStoreFileName(truststore_name)
                    .trustStorePassword(truststore_password.toCharArray())
                    .sslProtocol(ssl_protocol)
                    .provider(ssl_provider)
                    .build()
                    .sslContext();
            if (client_ssl_ctx == null) {
                client_ssl_ctx = sslContext;
                client_ssl_ctx_reloader.setContext(client_ssl_ctx).setFactory(sslContextFactory);
            }
            if (server_ssl_ctx == null) {
                server_ssl_ctx = sslContext;
                server_ssl_ctx_reloader.setContext(server_ssl_ctx).setFactory(sslContextFactory);
            }
        }

        if(session_verifier == null && session_verifier_class != null) {
            Class<? extends SessionVerifier> verifier_class=(Class<? extends SessionVerifier>)Util.loadClass(session_verifier_class, getClass());
            session_verifier=verifier_class.getDeclaredConstructor().newInstance();
            if(session_verifier_arg != null)
                session_verifier.init(session_verifier_arg);
        }
    }

    public void start() throws Exception {
        super.start();
    }

    public void stop() {
        super.stop();
        stopKeyserver();
    }

    @ManagedOperation
    public void reloadKeystoreAndTruststore() {
        client_ssl_ctx_reloader.reload(true);
        server_ssl_ctx_reloader.reload(true);
    }

    public void destroy() {
        super.destroy();
    }

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


    public void fetchSecretKeyFrom(Address target) throws Exception {
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


    protected void accept() {
        try(SSLSocket client_sock=(SSLSocket)srv_sock.accept()) {
            server_ssl_ctx_reloader.reload(); // try to reload ssl context before handshake
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
            log.trace("%s: failure handling client socket: %s", local_addr, t.getMessage());
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
                catch(Throwable e) {
                    log.error("failed becoming key server", e);
                }
            }
        }
        else { // stop being keyserver, close the server socket and handler
            if(Objects.equals(old_coord, local_addr))
                stopKeyserver();
        }
    }


    protected synchronized void becomeKeyserver() throws Exception {
        if(srv_sock == null || srv_sock.isClosed()) {
            log.debug("%s: becoming keyserver; creating server socket", local_addr);
            srv_sock=createServerSocket();
            srv_sock_handler=new Runner(getThreadFactory(), SSL_KEY_EXCHANGE.class.getSimpleName() + "-runner",
                                        this::accept, () -> Util.close(srv_sock));
            srv_sock_handler.start();
            log.debug("%s: SSL server socket listening on %s", local_addr, srv_sock.getLocalSocketAddress());
        }
    }

    protected synchronized void stopKeyserver() {
        if(srv_sock != null) {
            Util.close(srv_sock); // should not be necessary (check)
            srv_sock=null;
        }
        if(srv_sock_handler != null) {
            log.debug("%s: ceasing to be the keyserver; closing the server socket", local_addr);
            srv_sock_handler.stop();
            srv_sock_handler=null;
        }
    }


    protected SSLServerSocket createServerSocket() throws Exception {
        SSLServerSocketFactory sslServerSocketFactory=this.server_ssl_ctx.getServerSocketFactory();

        SSLServerSocket sslServerSocket;
        for(int i=0; i <= port_range; i++) {
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
        client_ssl_ctx_reloader.reload(); // try to reload the context before creating the socket
        SSLSocketFactory sslSocketFactory=this.client_ssl_ctx.getSocketFactory();

        if(target instanceof IpAddress)
            return createSocketTo((IpAddress)target, sslSocketFactory);

        IpAddress dest=(IpAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, target));
        SSLSocket sock;
        for(int i=0; i <= port_range; i++) {
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
        throw new IllegalStateException(String.format("failed connecting to %s (port range [%d - %d])",
                                                      dest.getIpAddress(), port, port+port_range));
    }

    protected SSLSocket createSocketTo(IpAddress dest, SSLSocketFactory sslSocketFactory) {
        try {
            SSLSocket sock=(SSLSocket)sslSocketFactory.createSocket(dest.getIpAddress(), dest.getPort());
            sock.setSoTimeout(socket_timeout);
            sock.setEnabledCipherSuites(sock.getSupportedCipherSuites());
            sock.startHandshake();
            SSLSession sslSession=sock.getSession();

            log.debug("%s: created SSL connection to %s (%s); protocol: %s, cipher suite: %s",
                      local_addr, dest, sock.getRemoteSocketAddress(), sslSession.getProtocol(), sslSession.getCipherSuite());

            if(session_verifier != null)
                session_verifier.verify(sslSession);
            return sock;
        }
        catch(SecurityException sec_ex) {
            throw sec_ex;
        }
        catch(Throwable t) {
            throw new IllegalStateException(String.format("failed connecting to %s: %s", dest, t.getMessage()));
        }
    }
}
