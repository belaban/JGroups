package org.jgroups.tests;

import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;

/**
 * @author Bela Ban
 * @version $Id: LargeStateClient.java,v 1.1 2006/05/26 06:21:13 belaban Exp $
 */
public class LargeStateClient {
    Socket       sock;
    InetAddress  bind_addr, host_addr;
    boolean      looping=true;


    private void start(String bind_addr, String host_addr, int chunk) throws Exception {
        this.bind_addr=InetAddress.getByName(bind_addr);
        this.host_addr=InetAddress.getByName(host_addr);
        sock=new Socket(this.host_addr, 7500, this.bind_addr, 0);
        sock.setReceiveBufferSize(chunk);
        readLargeState(sock);
        sock.close();
    }

    private void readLargeState(Socket sock) throws IOException {
        InputStream in=sock.getInputStream();
        long total=0, start=0, stop;
        byte[] buf=new byte[sock.getReceiveBufferSize()];
        int num;

        start=System.currentTimeMillis();
        while(looping) {
            num=in.read(buf, 0, buf.length);
            if(num == -1)
                break;

            total+=num;
            if(total % 100000000 == 0)
                System.out.println("-- read " + (total / 1000000) + " MB");
        }
        stop=System.currentTimeMillis();
        System.out.println("- done, read " + (total / 1000000) + " MB in " + (stop-start) + "ms (" +
                (total / 1000000) / ((stop-start) / 1000.0) + " MB/sec");
    }


    public static void main(String[] args) throws Exception {
        String bind_addr=null, host="localhost";
        int chunk=10000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-bind_addr")) {
                bind_addr=args[++i];
                continue;
            }
            if(args[i].equals("-host")) {
                host=args[++i];
                continue;
            }
            if(args[i].equals("-chunk")) {
                chunk=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("LargeStateServer [-bind_addr <addr>] [-host <host address>][-chunk <chunk size (bytes)>]");
            return;
        }
        new LargeStateClient().start(bind_addr, host, chunk);
    }


}
