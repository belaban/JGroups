package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.Serializable;

/**
 * Simple test for multicast discovery. Use with ./conf/bare-bones.xml (without the UNICAST protocol).
 * Has no error checking and only rudimentary tracing (System.err :-))
 * @author Bela Ban
 * @version $Id: DiscoveryTest.java,v 1.2 2009/04/09 09:11:20 belaban Exp $
 */
public class DiscoveryTest {
    Channel ch;

    public static void main(String[] args) throws Exception {
        String props=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("DiscoveryTest [-props <properties>]");
        }

        new DiscoveryTest().start(props);
    }

    private void start(String props) throws Exception {
        ch=new JChannel(props);
        ch.connect("discovery");

        new Thread() {
            public void run() {
                while(true) {
                    try {
                        ch.send(null, null, new Data(Data.DISCOVERY_REQ, null));
                    }
                    catch(ChannelNotConnectedException e) {
                        e.printStackTrace();
                    }
                    catch(ChannelClosedException e) {
                        e.printStackTrace();
                    }
                    Util.sleep(5000);
                }
            }
        }.start();

        Object obj;
        Data d;
        Message msg;
        while(ch.isConnected()) {
            obj=ch.receive(0);
            if(obj instanceof Message) {
                msg=(Message)obj;
                d=(Data)msg.getObject();
                switch(d.type) {
                    case Data.DISCOVERY_REQ:
                        ch.send(msg.getSrc(), null, new Data(Data.DISCOVEY_RSP,
                                                             " my address is " + ch.getAddress()));
                        break;
                    case Data.DISCOVEY_RSP:
                        Address sender=msg.getSrc();

                        System.out.println("received response from " + sender + ": " + d.payload);
                        break;
                    default:
                        System.err.println("type " + d.type + " not known");
                        break;
                }
            }
        }
        ch.close();
    }


    private static class Data implements Serializable {
        private static final int DISCOVERY_REQ = 1;
        private static final int DISCOVEY_RSP  = 2;
        private static final long serialVersionUID = 9193522684840201133L;

        int type=0;
        Serializable payload=null;


        public Data(int type, Serializable payload) {
            this.type=type;
            this.payload=payload;
        }

    }
}
