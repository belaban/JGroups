package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;

/**
 * Tests for stale connection handling in BaseServer.
 * <ul>
 *   <li>Test 1: getConnection() cleans up a stale outgoing connection</li>
 *   <li>Test 2: isStale() returns true for connections older than sock_conn_timeout</li>
 * </ul>
 * @see <a href="https://issues.redhat.com/browse/JGRP-2968">JGRP-2968</a>
 */
@Test(groups=Global.FUNCTIONAL, singleThreaded=true)
public class StaleConnectionTest {
    protected TcpServer serverA, serverB;

    @AfterMethod
    protected void destroy() {
        Util.close(serverA, serverB);
    }

    /**
     * getConnection() should remove and close a stale connection before creating a new one.
     * A connects to B, B restarts (killing the socket), A.getConnection(B) should
     * detect the dead connection, clean it up, and create a fresh one.
     */
    public void testGetConnectionCleansUpStaleOutgoing() throws Exception {
        InetAddress loopback=Util.getLoopback();
        serverA=new TcpServer(loopback, 0);
        serverB=new TcpServer(loopback, 0);
        serverA.start();
        serverB.start();

        IpAddress addrB=(IpAddress)serverB.localAddress();

        Connection conn1=serverA.getConnection(addrB);
        assert conn1 != null && !conn1.isClosed() : "initial connection should be open";

        // Restart B → kills the old socket from B's side
        int bPort=addrB.getPort();
        serverB.stop();
        Util.sleep(300);
        serverB=new TcpServer(loopback, bPort);
        serverB.start();
        Util.sleep(500);

        // getConnection should detect the dead connection, remove it, and create a new one
        Connection conn2=serverA.getConnection(addrB);
        assert conn2 != null && !conn2.isClosed() : "new connection should be open";
        assert conn2 != conn1 : "should be a different connection object";
    }

    /**
     * isStale() should return true for connections whose last_access exceeds sock_conn_timeout.
     * Uses a single server — creates a connection, then waits past the threshold.
     */
    public void testIsStaleDetectsOldConnection() throws Exception {
        InetAddress loopback=Util.getLoopback();
        serverA=new TcpServer(loopback, 0);
        serverB=new TcpServer(loopback, 0);
        serverA.start();
        serverB.start();

        IpAddress addrB=(IpAddress)serverB.localAddress();

        // Create a connection — its last_access is set to now
        Connection conn=serverA.getConnection(addrB);
        assert conn != null;
        assert !serverA.isStale(conn) : "fresh connection should not be stale";

        // Wait longer than sock_conn_timeout (default 1000ms)
        int timeout=serverA.socketConnectionTimeout();
        Util.sleep(timeout + 500);

        // Now last_access is older than sock_conn_timeout → stale
        assert serverA.isStale(conn) : "old connection should be detected as stale";
    }
}
