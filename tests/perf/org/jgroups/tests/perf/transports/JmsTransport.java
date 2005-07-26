package org.jgroups.tests.perf.transports;

import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Properties;
import java.util.Map;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: JmsTransport.java,v 1.4 2005/07/26 11:50:21 belaban Exp $
 */
public class JmsTransport implements Transport, MessageListener {
    Receiver          receiver=null;
    Properties        config=null;
    Object            local_addr=null;
    ConnectionFactory factory;
    InitialContext    ctx;
    TopicConnection   conn;
    TopicSession      session;
    TopicPublisher    pub;
    TopicSubscriber   sub;
    Topic             topic;
    String            topic_name="topic/testTopic";


    public JmsTransport() {
    }

    public Object getLocalAddress() {
        return local_addr;
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;

        String tmp=config.getProperty("topic");
        if(tmp != null)
            topic_name=tmp;

        ctx=new InitialContext();
        factory=(ConnectionFactory)ctx.lookup("ConnectionFactory");


        // local_addr=new IpAddress(ucast_sock.getLocalAddress(), ucast_sock.getLocalPort());
        System.out.println("-- local_addr is " + local_addr);
    }


    public void start() throws Exception {
        this.local_addr=conn.getClientID();
        conn=((TopicConnectionFactory)factory).createTopicConnection();
        session=conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic=(Topic)ctx.lookup(topic_name);
        pub=session.createPublisher(topic);
        sub=session.createSubscriber(topic);
        sub.setMessageListener(this);
        conn.start();
    }

    public void stop() {
        try {
            conn.stop();
        }
        catch(JMSException e) {
            e.printStackTrace();
        }
    }

    public void destroy() {
    }

    public void setReceiver(Receiver r) {
        this.receiver=r;
    }

    public Map dumpStats() {
        return null;
    }

    public void send(Object destination, byte[] payload) throws Exception {
        if(destination != null)
            throw new Exception("JmsTransport.send(): unicast destination is not supported");
        BytesMessage msg=session.createBytesMessage();

        //todo: write the sender (maybe use ObjectMessage instead of BytesMessage)

        msg.writeInt(payload.length);
        msg.writeBytes(payload, 0, payload.length);
        pub.publish(topic, msg);
    }

    public void onMessage(Message message) {
        Object sender=null;
        if(message == null || !(message instanceof BytesMessage)) {
            System.err.println("JmsTransport.onMessage(): received a non BytesMessage (" + message + "), discarding");
            return;
        }
        BytesMessage msg=(BytesMessage)message;
        try {

          //  todo: read the sender

            int len=msg.readInt();
            byte[] payload=new byte[len];
            msg.readBytes(payload, len);
            if(receiver != null)
                receiver.receive(sender, payload);
        }
        catch(JMSException e) {
            e.printStackTrace();
        }

    }


}
