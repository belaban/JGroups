package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.blocks.cs.TcpClient;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadPool;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests that {@code sock_conn_timeout} also bounds the TLS handshake of an outgoing TCP connection when the peer
 * accepts the connection, but never answers the handshake (without a timer: each read is bounded by SO_TIMEOUT),
 * or answers so slowly that no single read times out (with a timer: the handshake as a whole is bounded).
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class TcpTlsHandshakeTimeoutTest {
    protected static final int CONN_TIMEOUT=500;

    protected ServerSocket       srv_sock;
    protected TcpClient          client;
    protected volatile boolean   trickle;
    protected TimeScheduler3     timer;
    protected final List<Socket> accepted=new ArrayList<>();

    @BeforeMethod protected void init() throws Exception {
        srv_sock=new ServerSocket(0);
        Thread acceptor=new Thread(() -> {
            try {
                for(;;) {
                    Socket s=srv_sock.accept();
                    synchronized(accepted) {
                        accepted.add(s);
                    }
                    if(trickle) { // then sends a byte every 100 ms, so that no single read ever times out
                        Thread t=new Thread(() -> {
                            try {
                                // valid TLS handshake record header announcing 16 KB, followed by a slow payload
                                s.getOutputStream().write(new byte[]{0x16, 0x03, 0x03, 0x40, 0x00});
                                for(;;) {
                                    s.getOutputStream().write(0);
                                    s.getOutputStream().flush();
                                    Util.sleep(100);
                                }
                            }
                            catch(IOException ignored) {
                            }
                        });
                        t.setDaemon(true);
                        t.start();
                    }
                }
            }
            catch(IOException ignored) {
            }
        });
        acceptor.setDaemon(true);
        acceptor.start();
    }

    @AfterMethod protected void destroy() {
        Util.close(client, srv_sock);
        if(timer != null)
            timer.stop();
        synchronized(accepted) {
            accepted.forEach(Util::close);
        }
    }

    public void testHandshakeIsBoundedByConnectTimeout() throws Exception {
        assertBounded();
    }

    public void testHandshakeWithTimerIsBoundedByConnectTimeout() throws Exception {
        timer=new TimeScheduler3(new ThreadPool(), new DefaultThreadFactory("timer", true), true);
        assertBounded();
    }

    // only a timer bounds the handshake as a whole; SO_TIMEOUT alone doesn't catch a peer that trickles bytes
    public void testHandshakeWithTricklingPeerIsBoundedByTimer() throws Exception {
        timer=new TimeScheduler3(new ThreadPool(), new DefaultThreadFactory("timer", true), true);
        trickle=true;
        assertBounded();
    }

    protected void assertBounded() throws Exception {
        client=new TcpClient(null, 0, Util.getLoopback(), srv_sock.getLocalPort());
        client.socketFactory(new DefaultSocketFactory(SSLContext.getDefault()));
        client.socketConnectionTimeout(CONN_TIMEOUT);
        if(timer != null)
            client.timer(timer);

        // start() runs in a separate thread, so that the test fails (and doesn't hang) if the handshake isn't bounded
        CompletableFuture<Void> future=CompletableFuture.runAsync(() -> {
            try {
                client.start();
            }
            catch(Exception ex) {
                throw new CompletionException(ex);
            }
        });
        try {
            future.get(CONN_TIMEOUT * 10L, TimeUnit.MILLISECONDS);
            assert false : "start() should have failed as the TLS handshake never completes";
        }
        catch(TimeoutException ex) {
            assert false : String.format("TLS handshake not bounded by sock_conn_timeout (%d ms)", CONN_TIMEOUT);
        }
        catch(ExecutionException expected) {
            // handshake was aborted after the timeout
        }
    }
}
