package org.jgroups.tests.adaptudp;

import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.List;



/**  Javagroups version used was 2.0.3. Recompiled and tested again with 2.0.6.
 *   JGroupsTester:
 *   1. Instantiates a JChannel object and joins the group.
 *       Partition properties conf. is the same as in the JBoss
 *       default configuration except for min_wait_time parameter
 *       that causes the following error:
 *			UNICAST.setProperties():
 *			these properties are not recognized:
 *			-- listing properties --
 *			   min_wait_time=2000
 *   2. Starts receiving until it receives a view change message
 *       with the expected number of members.
 *   3. Starts the receiver thread and if(sender), the sender thread.
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)
 */
public class UdpTester {
    private boolean sender;
    private int num_msgs;
    private int msg_size;
    private int num_senders;
    private long log_interval=1000;
    MulticastSocket recv_sock;
    DatagramSocket send_sock;
    int num_members;
    IpAddress local_addr;
    MyReceiver receiver=null;

    /** List<Address> . Contains member addresses */
    List members=new ArrayList();

    public UdpTester(MulticastSocket recv_sock, DatagramSocket send_sock, boolean snd, int num_msgs,
                     int msg_size, int num_members, int ns, long log_interval) {
        sender=snd;
        this.num_msgs=num_msgs;
        this.msg_size=msg_size;
        num_senders=ns;
        this.num_members=num_members;
        this.log_interval=log_interval;
        this.recv_sock=recv_sock;
        this.send_sock=send_sock;
        this.local_addr=new IpAddress(send_sock.getLocalAddress(), send_sock.getLocalPort());
    }

    public void initialize() throws Exception {

        waitUntilAllMembersHaveJoined();
        Util.sleep(1000);

        new ReceiverThread(recv_sock, num_msgs, msg_size, num_senders, log_interval).start();
        if(sender) {
            new SenderThread(send_sock, num_msgs, msg_size, log_interval).start();
        }
    }

    void waitUntilAllMembersHaveJoined() throws Exception {
        discoverExistingMembers();

    }

    private void discoverExistingMembers() throws Exception {
        receiver=new MyReceiver();
        members.clear();
        receiver.start();
        receiver.discoverExistingMembers();
        receiver.sendMyAddress();
        receiver.waitUntilAllMembersHaveJoined();

        // clear recv_sock
    }

    class MyReceiver extends Thread {
        boolean running=true;

        public void run() {
            byte[] buf=new byte[65000];
            DatagramPacket p=new DatagramPacket(buf, buf.length);
            ByteArrayInputStream input;
            ObjectInputStream in;
            Request req;
            boolean running=true;

            while(running) {
                try {
                    recv_sock.receive(p);
                    input=new ByteArrayInputStream(p.getData(), 0, p.getLength());
                    in=new ObjectInputStream(input);
                    req=(Request)in.readObject();
                    switch(req.type) {
                        case Request.DISCOVERY_REQ:
                            byte[] tmp;
                            ByteArrayOutputStream output=new ByteArrayOutputStream();
                            ObjectOutputStream out=new ObjectOutputStream(output);
                            Request rsp=new Request(Request.NEW_MEMBER, local_addr);
                            out.writeObject(rsp);
                            output.flush();
                            tmp=output.toByteArray();
                            DatagramPacket rsp_p=new DatagramPacket(tmp, tmp.length,
                                    InetAddress.getByName(Test.mcast_addr), Test.mcast_port);
                            send_sock.send(rsp_p);
                            break;
                        case Request.NEW_MEMBER:
                            IpAddress new_mbr=(IpAddress)req.arg;
                            if(!members.contains(new_mbr)) {
                                members.add(new_mbr);
                                System.out.println("-- discovered " + new_mbr);
                                if(members.size() >= num_members) {
                                    System.out.println("-- all members have joined (" + members + ')');
                                    running=false;
                                    break;
                                }
                            }
                            break;
                        default:
                            System.err.println("don't recognize request with type=" + req.type);
                            break;
                    }
                }
                catch(IOException e) {
                    e.printStackTrace();
                    break;
                }
                catch(ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        public void discoverExistingMembers() throws Exception {
            Request req=new Request(Request.DISCOVERY_REQ, null);
            byte[] b=Util.objectToByteBuffer(req);
            DatagramPacket p=new DatagramPacket(b, b.length, InetAddress.getByName(Test.mcast_addr), Test.mcast_port);
            send_sock.send(p);

        }

        public void sendMyAddress() throws Exception {
            Request req=new Request(Request.NEW_MEMBER, local_addr);
            byte[] b=Util.objectToByteBuffer(req);
            DatagramPacket p=new DatagramPacket(b, b.length, InetAddress.getByName(Test.mcast_addr), Test.mcast_port);
            send_sock.send(p);
        }

        public void waitUntilAllMembersHaveJoined() throws InterruptedException {
            if(members.size() < num_members) {
                if(receiver.isAlive())
                    receiver.join();
            }
        }

    }

}

