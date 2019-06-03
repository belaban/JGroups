package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.blocks.cs.TcpClient;
import org.jgroups.util.Runner;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Tests https://issues.jboss.org/browse/JGRP-2350
 * @author Bela Ban
 * @since  4.1.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class TcpServerTest {
    protected TcpClient    client;
    protected ServerSocket srv_sock;
    protected Runner       runner;
    protected long         timestamp;


    @BeforeMethod protected void init() throws Exception {
        srv_sock=new ServerSocket(0);
    // Util.createServerSocket(new DefaultSocketFactory(), "srv-socket", null, 7500, 7600);
        assert !srv_sock.isClosed();
    }

    @AfterMethod protected void destroy() {Util.close(client, srv_sock, runner);}


    public void testCloseOnBlockedSend() throws Exception {
        // function which accepts connections, but doesn't read data off of the accepted socket
        Runnable acceptor=() -> {
            Socket client_sock=null;
            try {
                client_sock=srv_sock.accept();
                for(;;) {
                    Util.sleep(5000);
                }
            }
            catch(IOException e) {
            }
            finally {
                Util.close(client_sock);
            }
        };

        Runnable writer=() -> {
            byte[] array=new byte[8192*4];
            int    total=0;
            try {
                client=new TcpClient(null, 0, Util.getLoopback(), srv_sock.getLocalPort());
                client.start();
                for(;;) {
                    client.send(array, 0, array.length);
                    total+=array.length;
                    System.out.printf("- wrote %d bytes\n", total);
                    timestamp=System.currentTimeMillis();
                }
            }
            catch(Exception e) {
            }
            finally {
                Util.close(client);
            }
        };

        Runnable closer=() -> Util.close(client);

        Thread acceptor_thread=new Thread(acceptor);
        acceptor_thread.start();
        Util.sleep(500);

        Thread writer_thread=new Thread(writer);
        writer_thread.start();

        Thread closer_thread=new Thread(closer);
        // Only start when the writer thread has been blocked for 2s:
        while(timestamp == 0 || System.currentTimeMillis() - timestamp < 2000) {
            Util.sleep(100);
        }
        closer_thread.start();

        // assert that closer_thread returns successfully:
        for(int i=0; i < 10; i++) {
            if(!closer_thread.isAlive())
                break;
            Util.sleep(1000);
        }

        assert !closer_thread.isAlive();
        assert !client.isConnected();
        assert !writer_thread.isAlive();
    }

}
