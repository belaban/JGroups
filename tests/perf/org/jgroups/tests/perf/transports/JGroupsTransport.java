package org.jgroups.tests.perf.transports;

import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;
import org.jgroups.*;

import java.util.Properties;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: JGroupsTransport.java,v 1.1 2004/01/23 00:08:30 belaban Exp $
 */
public class JGroupsTransport implements Transport, Runnable {
    Properties config=null;
    JChannel   channel=null;
    Thread     t=null;
    String     props=null;
    String     group_name="PerfGroup";
    Receiver   receiver=null;

    public JGroupsTransport() {

    }

    public Object getLocalAddress() {
        return channel != null? channel.getLocalAddress() : null;
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;
        props=config.getProperty("props");
        channel=new JChannel(props);
    }

    public void start() throws Exception {
        channel.connect(group_name);
        t=new Thread(this, "JGroupsTransport receiver thread");
        t.start();
    }

    public void stop() {
        if(channel != null) {
            channel.disconnect(); // will cause thread to terminate anyways
        }
        t=null;
    }

    public void destroy() {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }

    public void setReceiver(Receiver r) {
        this.receiver=r;
    }

    public void send(Object destination, byte[] payload) throws Exception {
        Message msg=new Message((Address)destination, null, payload);
        channel.send(msg);
    }

    public void run() {
        Object obj;
        Message msg;
        Object sender;
        byte[] payload;

        while(t != null) {
            try {
                obj=channel.receive(0);
                if(obj instanceof Message) {
                    msg=(Message)obj;
                    sender=msg.getSrc();
                    payload=msg.getBuffer();
                    if(receiver != null) {
                        try {
                            receiver.receive(sender, payload);
                        }
                        catch(Throwable t) {
                            t.printStackTrace();
                        }
                    }
                }
            }
            catch(ChannelNotConnectedException e) {
                e.printStackTrace();
                t=null;
            }
            catch(ChannelClosedException e) {
                e.printStackTrace();
                t=null;
            }
            catch(TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}
