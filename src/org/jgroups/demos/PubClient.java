package org.jgroups.demos;

import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.*;
import org.jgroups.util.Util;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;

/**
 * Demo of a client which connects to a server ({@link PubServer}) and sends it messages which are forwarded to all
 * connected clients
 * @author Bela Ban
 * @since  3.6.5
 */
public class PubClient implements Receiver, ConnectionListener {
    protected BaseServer       client;
    protected final String     name;
    protected volatile boolean running=true;
    protected InputStream      in;

    public PubClient(String name) {
        this.name=name;
    }


    @Override
    public void receive(PhysicalAddress sender, ByteBuffer buf) {
        byte[] buffer=buf.array();
        String msg=new String(buffer, buf.arrayOffset(), buf.remaining());
        System.out.printf("-- %s\n", msg);
    }

    @Override
    public void receive(PhysicalAddress sender, byte[] buf, int offset, int length) {
        String msg=new String(buf, offset, length);
        System.out.printf("-- %s\n", msg);
    }

    @Override public void receive(PhysicalAddress sender, DataInput in, int length) throws Exception {
        byte[] buf=new byte[length];
        in.readFully(buf);
        String msg=new String(buf);
        System.out.printf("-- %s\n", msg);
    }

    @Override
    public void connectionClosed(Connection conn) {
        client.stop();
        running=false;
        Util.close(in);
        System.out.printf("connection to %s closed", conn.peerAddress());
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }

    protected void start(InetAddress srv_addr, int srv_port, boolean nio) throws Exception {
        client=nio? new NioClient(InetAddress.getLocalHost(), 0, srv_addr, srv_port)
          : new TcpClient(InetAddress.getLocalHost(), 0, srv_addr, srv_port);
        client.receiver(this);
        client.addConnectionListener(this);
        client.start();
        send(String.format("%s joined", name));
        eventLoop();
        client.stop();
    }

    protected void eventLoop() {
        in=new BufferedInputStream(System.in);
        while(running) {
            try {
                System.out.print("> "); System.out.flush();
                String line=Util.readLine(in);
                if(line == null)
                    break;
                if(line.startsWith("quit") || line.startsWith("exit")) {
                    break;
                }
                send(String.format("%s: %s", name, line));
            }
            catch(Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    protected void send(String str) throws Exception {
        byte[] buf=str.getBytes();
        ((Client)client).send(buf, 0, buf.length);
    }

    public static void main(String[] args) throws Exception {
        InetAddress server_addr=InetAddress.getLocalHost();
        int         server_port=7500;
        String      name=null;
        boolean     nio=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-addr")) {
                server_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                server_port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-nio")) {
                nio=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        if(name == null)
            throw new IllegalStateException("name needs to be defined");

        PubClient client=new PubClient(name);
        client.start(server_addr, server_port, nio);
    }



    protected static void help() {
        System.out.println("PubClient -name name [-addr <server address>] [-port <server port>] [-nio true|false]");
    }
}
