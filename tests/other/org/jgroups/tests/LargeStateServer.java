package org.jgroups.tests;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author Bela Ban
 * @version $Id: LargeStateServer.java,v 1.1 2006/05/26 06:21:13 belaban Exp $
 */
public class LargeStateServer {
    ServerSocket srv_sock;
    InetAddress  bind_addr;


    private void start(String bind_addr, int chunk_size) throws Exception {
        this.bind_addr=InetAddress.getByName(bind_addr);
        srv_sock=new ServerSocket(7500, 50, this.bind_addr);
        while(true) {
            System.out.println("-- waiting for clients to connect");
            Socket sock=srv_sock.accept();
            sock.setSendBufferSize(chunk_size);
            sendLargeState(sock);
            sock.close();
        }
    }

    private void sendLargeState(Socket sock) throws IOException {
        long total=0, start, stop;
        OutputStream out=sock.getOutputStream();
        start=System.currentTimeMillis();
        for(int i=0; i < 1000000; i++) {
            byte[] buf=new byte[1000];
            out.write(buf, 0, buf.length);
            total+=1000;
            if(total % 100000000 == 0)
                System.out.println("-- sent " + (total / 1000000) + " MB");
        }
        stop=System.currentTimeMillis();
        out.close();
        System.out.println("- done, wrote " + (total / 1000000) + " MB in " + (stop-start) + "ms (" +
                (total / 1000000) / ((stop-start) / 1000.0) + " MB/sec");
    }

    public static void main(String[] args) throws Exception {
        String bind_addr=null;
        int chunk=10000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-bind_addr")) {
                bind_addr=args[++i];
                continue;
            }
            if(args[i].equals("-chunk")) {
                chunk=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("LargeStateServer [-bind_addr <addr>] [-chunk <size in bytes>]");
            return;
        }
        new LargeStateServer().start(bind_addr, chunk);
    }


}
