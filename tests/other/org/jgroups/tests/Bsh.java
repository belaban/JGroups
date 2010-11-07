// $Id: Bsh.java,v 1.10 2010/03/05 11:55:35 belaban Exp $


package org.jgroups.tests;


import org.jgroups.util.Util;

import java.io.*;
import java.net.Socket;


/**
 * Interactive program to test a unicast channel
 * @author Bela Ban March 16 2003
 */
public class Bsh {
    String   host="localhost";
    int      port=0;
    long     timeout=0;


    public void start(String[] args) throws Exception {

        for(int i=0; i < args.length; i++) {
            String tmp=args[i];

            if("-host".equals(tmp)) {
                host=args[++i];
                continue;
            }

            if("-port".equals(tmp)) {
                port=Integer.parseInt(args[++i]);
                continue;
            }

            if("-timeout".equals(tmp)) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }

            help();
            return;
        }


        runClient();
    }

    void runClient() throws Exception {
        final Socket sock=new Socket(host, port);
        final InputStream in=sock.getInputStream();
        final OutputStream out=sock.getOutputStream();
        final BufferedReader reader= new BufferedReader(new InputStreamReader(System.in));

        new Thread() {
            public void run() {

                byte[] buf=new byte[1024];
                while(!sock.isClosed()) {
                    try {
                        int num=in.read(buf, 0, buf.length);
                        String str=new String(buf, 0, num);
                        System.out.println(str);
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        }.start();

        while(true) {
            System.out.print("> ");
            String line=reader.readLine();
            if(line.startsWith("quit") || line.startsWith("exit")) {
                Util.close(sock);
                return;
            }
            line=line + "\n";
            byte[] buf=line.getBytes();
            out.write(buf, 0, buf.length);
            out.flush();
        }
    }




    static void help() {
        System.out.println("Bsh [-help] [-host <host>] [-port <port>] [-timeout <timeout>]");
    }


    public static void main(String[] args) {
        try {
            new Bsh().start(args);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

}
