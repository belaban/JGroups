package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.util.Date;

/**
 * @author Bela Ban
 * @version $Id: RelayTest.java,v 1.1 2008/03/03 12:34:41 belaban Exp $
 */
public class RelayTest {
    private ServerSocket srv_sock;
    private Socket sock;
    private InetAddress local_addr=null, remote_addr=null;
    private OutputStream remote_out=null;
    private InputStream remote_in=null;
    private int local_port=5000, remote_port=5000;
    private int size=1000 * 1000; // 1MB
    private State state=null;
    private static final int CHUNK_SIZE=60000;


    private void start(String[] args) throws Exception {
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-local_addr")) {
                local_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-addr")) {
                remote_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-local_port")) {
                local_port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                remote_port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-size")) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        if(local_addr != null) {
            srv_sock=new ServerSocket(local_port, 0, local_addr);
        }

        if(remote_addr != null) {
            sock=new Socket(remote_addr, remote_port);
            remote_out=sock.getOutputStream();
            remote_in=sock.getInputStream();
        }

        if(local_addr != null)
            System.out.println("local_addr: " + local_addr + ":" + local_port);

        if(remote_addr != null)
            System.out.println("remote_addr: " + remote_addr + ":" + remote_port);

        sanityCheck();
        System.out.println("state = " + state);

        switch(state) {
            case sender:
                loop();
                break;
            case terminator:
            case forwarder:
                handleInput();
                break;
        }
    }

    private void handleInput() throws IOException {
        Socket client_sock=null;
        InputStream inp=null;
        OutputStream out=null;
        final byte[] buf=new byte[CHUNK_SIZE];
        int cnt=0, tmp;

        while((client_sock=srv_sock.accept()) != null) {
            try {
                inp=client_sock.getInputStream();
                out=client_sock.getOutputStream();
                while((tmp=inp.read(buf, 0, buf.length)) != -1) {
                    cnt+=tmp;
                    if(state == State.forwarder) {
                        remote_out.write(buf, 0, tmp);
                    }

                    if(cnt >= size) {
                        if(state == State.terminator) {
                            System.out.println("received " + Util.printBytes(cnt) + " at " + new Date());
                        }
                        else {
                            remote_in.read();
                        }
                        cnt=0;
                        out.write('1'); out.flush();
                    }
                }
            }
            finally {
                client_sock.close();
            }
        }
    }

    private void loop() throws IOException {
        int ch;
        while(true) {
            System.out.println("[1] send " + Util.printBytes(size) + " data [x] Exit");
            ch=System.in.read();
            switch(ch) {
                case -1:
                    break;
                case 'x':
                case 'X':
                    break;
                case '1':
                    send();
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Sends size bytes in chunkd sof CHUNK_SIZE bytes to remote_addr:remote_port
     */
    private void send() throws IOException {
        int cnt=0;
        byte[] buf=new byte[CHUNK_SIZE];
        System.out.println("-- sending " + Util.printBytes(size) + " to " + remote_addr + ":" + remote_port);
        long start=System.currentTimeMillis();
        while(cnt + CHUNK_SIZE < size) {
            remote_out.write(buf, 0, buf.length);
            cnt+=CHUNK_SIZE;
        }
        if(cnt < size)
            remote_out.write(buf, 0, size-cnt);
        remote_out.flush();

        remote_in.read();
        long diff=System.currentTimeMillis() - start;
        System.out.println("sent " + Util.printBytes(size) + " in " + diff + " ms");
    }

    private void sanityCheck() {
        if(srv_sock == null && sock != null)
            state=State.sender;
        else if(srv_sock != null && sock == null)
            state=State.terminator;
        else if(srv_sock != null && sock != null)
            state=State.forwarder;
        else
            throw new IllegalStateException("either server socket or socket has to be non-null");
    }


    public static void main(String[] args) throws Exception {
        new RelayTest().start(args);
    }

    static void help() {
        System.out.println("RelayTest [-help] [-local_addr <bind addr>] [-local_port <port>] " +
                "[-addr <remote address>] [-port <remote port>] [-size <num bytes to send/forward>]");
    }

    static enum State {sender, forwarder, terminator};
}


