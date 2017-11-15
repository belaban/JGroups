
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Same as UnicastTest, but uses pure TCP instead of JGroups
 *
 * @author Bela Ban
 */
public class UnicastTestTcp {
    protected boolean              oob=false, dont_bundle=true;
    protected int                  num_threads=1;
    protected int                  num_msgs=100000, msg_size=1000;
    protected InetSocketAddress    local, remote;
    protected PhysicalAddress      destination;
    protected Socket               sock;
    protected DataOutputStream     output;
    protected final Lock           output_lock=new ReentrantLock();
    protected ServerSocket         srv_sock;
    protected Acceptor             acceptor;
    protected long                 start=0, stop=0;
    protected long                 total_time=0, msgs_per_sec, print;
    protected AtomicLong           current_value=new AtomicLong(0), total_bytes=new AtomicLong(0);

    protected static final boolean TCP_NODELAY=false;
    protected static final int     SOCK_SEND_BUF_SIZE=200 * 1000;
    protected static final int     SOCK_RECV_BUF_SIZE=200 * 1000;
    protected static final byte    START = 1; // | num_msgs (long) |
    protected static final byte    DATA  = 2; // | length (int) | data (byte[]) |




    public void init(String local_addr, String remote_addr, int local_port, int remote_port) throws Exception {
        local=new InetSocketAddress(local_addr, local_port);
        remote=new InetSocketAddress(remote_addr, remote_port);
        destination=new IpAddress(remote.getAddress(), remote.getPort());
        srv_sock=Util.createServerSocket(new DefaultSocketFactory(), "server", local.getAddress(), local.getPort());
        System.out.println("Listening on " + srv_sock.getLocalSocketAddress());
        acceptor=new Acceptor();
        acceptor.start();

        sock=new Socket();
        //sock.bind(local);
        sock.setSendBufferSize(SOCK_SEND_BUF_SIZE);
        sock.setReceiveBufferSize(SOCK_RECV_BUF_SIZE);
        try {
            sock.connect(remote);
            output=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
            System.out.println("Connected to " + sock.getRemoteSocketAddress());
        }
        catch(Throwable t) {
            System.out.println("Failed connecting to " + remote + ": will only act as server");
        }
    }


    public void eventLoop() throws Exception {
        int c;

        while(true) {
            System.out.print("[1] Send msgs " +
                               "\n[6]Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                    "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                    "\n[o] Toggle OOB (" + oob + ") [b] Toggle dont_bundle (" + dont_bundle + ")\n[q] Quit\n");
            System.out.flush();
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                sendMessages();
                break;
            case '6':
                setSenderThreads();
                break;
            case '7':
                setNumMessages();
                break;
            case '8':
                setMessageSize();
                break;
            case 'o':
                oob=!oob;
                System.out.println("oob=" + oob);
                break;
                case 'b':
                    dont_bundle=!dont_bundle;
                    System.out.println("dont_bundle = " + dont_bundle);
                    break;
            case 'q':
                Util.close(srv_sock);
                Util.close(sock);
                return;
            default:
                break;
            }
        }
    }




    void sendMessages() throws Exception {
        if(num_threads > 1 && num_msgs % num_threads != 0) {
            System.err.println("num_msgs (" + num_msgs + " ) has to be divisible by num_threads (" + num_threads + ")");
            return;
        }

        System.out.println("sending " + num_msgs + " messages (" + Util.printBytes(msg_size) +
                             ") to " + remote + ": oob=" + oob + ", " + num_threads + " sender thread(s)");
        ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE).put(START).putLong(num_msgs);
        Message msg=new BytesMessage(destination, buf.array());
        // msg.writeTo(output);
        writeMessage(msg, output);

        int msgs_per_sender=num_msgs / num_threads;
        Sender[] senders=new Sender[num_threads];
        for(int i=0; i < senders.length; i++)
            senders[i]=new Sender(msgs_per_sender, msg_size, num_msgs / 10);
        for(Sender sender: senders)
            sender.start();
        for(Sender sender: senders)
            sender.join();
        output.flush();
        System.out.println("done sending " + num_msgs + " to " + remote);
    }

    void setSenderThreads() throws Exception {
        int threads=Util.readIntFromStdin("Number of sender threads: ");
        int old=this.num_threads;
        this.num_threads=threads;
        System.out.println("sender threads set to " + num_threads + " (from " + old + ")");
    }

    void setNumMessages() throws Exception {
        num_msgs=Util.readIntFromStdin("Number of messages: ");
        System.out.println("Set num_msgs=" + num_msgs);
    }

    void setMessageSize() throws Exception {
        msg_size=Util.readIntFromStdin("Message size: ");
        System.out.println("set msg_size=" + msg_size);
    }


    public static void main(String[] args) {
        String  local_addr="localhost", remote_addr="localhost";
        int     local_port=8000, remote_port=9000;

        for(int i=0; i < args.length; i++) {
            if("-local_addr".equals(args[i])) {
                local_addr=args[++i];
                continue;
            }
            if("-remote_addr".equals(args[i])) {
                remote_addr=args[++i];
                continue;
            }
            if("-local_port".equals(args[i])) {
                local_port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-remote_port".equals(args[i])) {
                remote_port=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }


        try {
            UnicastTestTcp test=new UnicastTestTcp();
            test.init(local_addr, remote_addr, local_port, remote_port);
            test.eventLoop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("UnicastTest [-help] [-local_addr add] [-remote_addr addr] [-local_port port] [-remote_port port]");
    }


    public void receive(Message msg) {
        byte[] buf=msg.getArray();
        byte   type=buf[msg.getOffset()];

        switch(type) {
            case START:
                ByteBuffer tmp=ByteBuffer.wrap(buf, 1+msg.getOffset(), Global.LONG_SIZE);
                num_msgs=(int)tmp.getLong();
                print=num_msgs / 10;
                current_value.set(0);
                total_bytes.set(0);
                start=System.currentTimeMillis();
                break;
            case DATA:
                long new_val=current_value.incrementAndGet();
                total_bytes.addAndGet(msg.getLength() - Global.INT_SIZE);
                if(print > 0 && new_val % print == 0)
                    System.out.println("received " + new_val);
                if(new_val >= num_msgs) {
                    long time=System.currentTimeMillis() - start;
                    double msgs_sec=(current_value.get() / (time / 1000.0));
                    double throughput=total_bytes.get() / (time / 1000.0);
                    System.out.println(String.format("\nreceived %d messages in %d ms (%.2f msgs/sec), throughput=%s",
                                                     current_value.get(), time, msgs_sec, Util.printBytes(throughput)));
                    break;
                }
                break;
            default:
                System.err.println("Type " + type + " is invalid");
        }
    }

    protected static final void writeMessage(final Message msg, DataOutputStream out) throws Exception {
        byte flags=0;
        if(msg.getDest() == null)
            flags+=(byte)2;
        out.writeShort(Version.version); // write the version
        out.writeByte(flags);
        msg.writeTo(out);
    }



    protected static final Message readMessage(DataInput in) throws Exception {
        short ver=in.readShort();
        byte flags=in.readByte();
        // final boolean multicast=(flags & (byte)2) == (byte)2;
        Message msg=new BytesMessage(false); // don't create headers, readFrom() will do this
        msg.readFrom(in);
        return msg;
    }

    protected class Acceptor extends Thread {
        public void run() {
            while(!srv_sock.isClosed()) {
                Socket client_sock=null;
                DataInputStream in=null;
                try {
                    client_sock=srv_sock.accept();
                    client_sock.setTcpNoDelay(TCP_NODELAY);
                    client_sock.setReceiveBufferSize(SOCK_RECV_BUF_SIZE);
                    client_sock.setSendBufferSize(SOCK_SEND_BUF_SIZE);
                    in=new DataInputStream(new BufferedInputStream(client_sock.getInputStream()));
                    while(!client_sock.isClosed())
                        handleRequest(in);
                }
                catch(Exception e) {
                    Util.close(client_sock);
                    Util.close(in);
                }
            }
        }

        protected void handleRequest(DataInputStream in) throws Exception {
            Message msg=readMessage(in); // new BytesMessage(false);
            // msg.readFrom(in);
            receive(msg);
        }
    }




    private class Sender extends Thread {
        protected final int    number_of_msgs;
        protected final int    do_print;
        protected final byte[] buf;

        public Sender(int num_msgs, int msg_size, int print) {
            this.number_of_msgs=num_msgs;
            this.do_print=print;
            buf=ByteBuffer.allocate(Global.INT_SIZE + msg_size).put(DATA).array();
        }

        public void run() {
            for(int i=1; i <= number_of_msgs; i++) {
                try {
                    Message msg=new BytesMessage(destination, buf);
                    if(oob)
                        msg.setFlag(Message.Flag.OOB);
                    if(dont_bundle)
                        msg.setFlag(Message.Flag.DONT_BUNDLE);
                    if(i > 0 && do_print > 0 && i % do_print == 0)
                        System.out.println("-- sent " + i);
                    output_lock.lock(); // need to sync if we have more than 1 sender
                    try {
                        // msg.writeTo(output);
                        writeMessage(msg, output);
                    }
                    finally {
                        output_lock.unlock();
                    }
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }



}
