package org.jgroups.tests.perf.transports;

import org.jgroups.stack.IpAddress;
import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: TcpTransport.java,v 1.8 2005/07/01 08:05:12 belaban Exp $
 */
public class TcpTransport implements Transport {
    Receiver         receiver=null;
    Properties       config=null;
    int              max_receiver_buffer_size=500000;
    int              max_send_buffer_size=500000;
    List             nodes=new ArrayList();
    ConnectionTable  ct;
    int              srv_port=7777;
    ServerSocket     srv_sock=null;
    InetAddress      bind_addr=null;
    IpAddress        local_addr=null;
    List             receivers=new ArrayList();


    public TcpTransport() {
    }

    public Object getLocalAddress() {
        return local_addr;
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;
//        System.out.println("-- local_addr is " + local_addr);

        String tmp;
        if((tmp=config.getProperty("srv_port")) != null)
            srv_port=Integer.parseInt(tmp);

        String bind_addr_str=System.getProperty("udp.bind_addr", config.getProperty("bind_addr"));
        if(bind_addr_str != null) {
            bind_addr=InetAddress.getByName(bind_addr_str);
        }
        else
            bind_addr=InetAddress.getLocalHost();

        String cluster_def=config.getProperty("cluster");
        if(cluster_def == null)
            throw new Exception("TcpTransport.create(): property 'cluster' is not defined");
        nodes=parseCommaDelimitedList(cluster_def);
        ct=new ConnectionTable(nodes);
    }


    public void start() throws Exception {
        srv_sock=Util.createServerSocket(bind_addr, srv_port);
        local_addr=new IpAddress(srv_sock.getInetAddress(), srv_sock.getLocalPort());
        ct.init();

        // accept connections and start 1 Receiver per connection
        Thread acceptor=new Thread() {
            public void run() {
                while(true) {
                    try {
                        Socket s=srv_sock.accept();
                        ReceiverThread r=new ReceiverThread(s);
                        r.setDaemon(true);
                        receivers.add(r);
                        r.start();
                    }
                    catch(Exception ex) {
                        ex.printStackTrace();
                        break;
                    }
                }
            }
        };
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public void stop() {
        ct.close();
        for(Iterator it=receivers.iterator(); it.hasNext();) {
            ReceiverThread thread=(ReceiverThread)it.next();
            thread.stopThread();
        }
    }

    public void destroy() {
        ;
    }

    public void setReceiver(Receiver r) {
        this.receiver=r;
    }

    public void send(Object destination, byte[] payload) throws Exception {
        if(destination != null)
            throw new Exception("TcpTransport.send(): unicasts not supported");
        ct.writeMessage(payload);
    }


    class ConnectionTable {
        /** List<InetSocketAddress> */
        List myNodes;
        Connection[] connections;

        ConnectionTable(List nodes) throws Exception {
            this.myNodes=nodes;
            connections=new Connection[nodes.size()];
        }


        void init() throws Exception {
            int i=0;

            for(Iterator it=myNodes.iterator(); it.hasNext();) {
                InetSocketAddress addr=(InetSocketAddress)it.next();
                if(connections[i] == null) {
                    try {
                        connections[i]=new Connection(addr);
                        System.out.println("-- connected to " +addr);
                        System.out.flush();
                    }
                    catch(Exception all_others) {
                        throw all_others;
                    }
                }
                i++;
            }
        }

         // todo: parallelize
         void writeMessage(byte[] msg) throws Exception {
             for(int i=0; i < connections.length; i++) {
                 Connection c=connections[i];
                 if(c != null)
                     c.writeMessage(msg);
             }
         }

         void close() {
             for(int i=0; i < connections.length; i++) {
                 Connection c=connections[i];
                 if(c != null)
                     c.close();
             }
         }

         public String toString() {
             StringBuffer sb=new StringBuffer();
             for(Iterator it=myNodes.iterator(); it.hasNext();) {
                 InetAddress inetAddress=(InetAddress)it.next();
                 sb.append(inetAddress).append(' ');
             }
             return sb.toString();
         }
     }

     class Connection {
         Socket sock=null;
         DataOutputStream out;
         InetSocketAddress addr;

         Connection(InetSocketAddress addr) {
             this.addr=addr;
             if(!createSocket())
                 System.err.println("Failed to connect to " + addr);
         }

         boolean createSocket() {
             try {
                 sock=new Socket(addr.getAddress(), addr.getPort());
                 out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
                 return true;
             }
             catch(IOException ex) {
                 return false;
             }
         }

         void writeMessage(byte[] msg) throws Exception {
             if(sock == null) {
                 if(!createSocket()) {
                     return;
                 }
             }
             out.writeInt(msg.length);
             out.write(msg, 0, msg.length);
             out.flush();
         }

         void close() {
             try {
                 sock.close();
             }
             catch(Exception ex) {
             }
         }
     }



    class ReceiverThread extends Thread {
        Socket          sock;
        DataInputStream in;

        ReceiverThread(Socket sock) throws Exception {
            this.sock=sock;
            sock.setSoTimeout(5000);
            in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
        }

        public void run() {
            while(sock != null) {
                try {
                    int len=in.readInt();
                    byte[] buf=new byte[len];
                    in.readFully(buf, 0, len);
                    if(receiver != null)
                        receiver.receive(sock.getRemoteSocketAddress(), buf);
                }
                catch(EOFException eof) {
                    break;
                }
                catch(Exception ex) {
                    if(sock == null) return;
                    // ex.printStackTrace();
                }
            }
        }

        void stopThread() {
            try {
                sock.close();
                sock=null;
            }
            catch(Exception ex) {
            }
        }
    }



    public List parseCommaDelimitedList(String s) throws Exception {
        List retval=new ArrayList();
        StringTokenizer tok;
        String hostname, tmp;
        int    port;
        InetSocketAddress addr;
        int index;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            tmp=tok.nextToken();
            index=tmp.indexOf(':');
            if(index == -1)
                throw new Exception("host must be in format <host:port>, was " + tmp);
            hostname=tmp.substring(0, index);
            port=Integer.parseInt(tmp.substring(index+1));
            addr=new InetSocketAddress(hostname, port);
            retval.add(addr);
        }
        return retval;
    }


}
