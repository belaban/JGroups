package org.jgroups.tests.adaptjms;

import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import javax.jms.*;
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
 * @version $Id: JmsTester.java,v 1.1 2004/02/20 22:54:05 belaban Exp $
 */
public class JmsTester {
    private boolean sender;
    private int num_msgs;
    private int msg_size;
    private int num_senders;
    private long log_interval=1000;
    Connection conn;
    TopicSession session;
    TopicPublisher pub;
    Topic topic;
    int num_members;
    Object local_addr;
    MyReceiver receiver=null;

    /** List<Address> . Contains member addresses */
    List members=new ArrayList();



    public JmsTester(Connection conn, TopicSession session, Topic topic, TopicPublisher pub, boolean snd, int num_msgs,
                     int msg_size, int num_members, int ns, long log_interval) {
        sender=snd;
        this.num_msgs=num_msgs;
        this.msg_size=msg_size;
        num_senders=ns;
        this.num_members=num_members;
        this.log_interval=log_interval;
        this.conn=conn;
        this.session=session;
        this.topic=topic;
        this.pub=pub;
    }

    public void initialize() throws Exception {
        this.local_addr=conn.getClientID();
        waitUntilAllMembersHaveJoined();
        Util.sleep(1000);

        conn.start();
        new ReceiverThread(session, topic, num_msgs, msg_size, num_senders, log_interval).start();
        if(sender) {
            new SenderThread(session, pub, topic, num_msgs, msg_size, log_interval).start();
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
    }



    class MyReceiver implements MessageListener {
        boolean running=true;
        TopicSubscriber sub;


        public void start() throws JMSException {
            sub=session.createSubscriber(topic);
            sub.setMessageListener(this);
        }

        public void onMessage(Message message) {
            Request req;

            if(message instanceof ObjectMessage) {
                req=(Request)message;
                switch(req.type) {
                    case Request.DISCOVERY_REQ:
                        Request rsp=new Request(Request.NEW_MEMBER, local_addr);
                        ObjectMessage msg=null;
                        try {
                            msg=session.createObjectMessage(rsp);
                            pub.publish(msg);
                        }
                        catch(JMSException e) {
                            e.printStackTrace();
                        }
                        break;
                    case Request.NEW_MEMBER:
                        IpAddress new_mbr=(IpAddress)req.arg;
                        if(!members.contains(new_mbr)) {
                            members.add(new_mbr);
                            System.out.println("-- discovered " + new_mbr);
                            if(members.size() >= num_members) {
                                System.out.println("-- all members have joined (" + members + ")");
                                running=false;
                                synchronized(this) {
                                    if(sub != null) {
                                        try {
                                            sub.setMessageListener(null);
                                        }
                                        catch(JMSException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    this.notifyAll();
                                }
                                break;
                            }
                        }
                        break;
                    default:
                        System.err.println("don't recognize request with type=" + req.type);
                        break;
                }
            }
        }


        public void discoverExistingMembers() throws Exception {
            Request req=new Request(Request.DISCOVERY_REQ, null);
            ObjectMessage msg=session.createObjectMessage(req);
            pub.publish(msg);
        }

        public void sendMyAddress() throws Exception {
            Request req=new Request(Request.NEW_MEMBER, local_addr);
            ObjectMessage msg=session.createObjectMessage(req);
            pub.publish(msg);
        }

        public void waitUntilAllMembersHaveJoined() throws InterruptedException {
            if(members.size() < num_members) {
                synchronized(receiver) {
                    receiver.wait();
                }
            }
        }

    }

}

