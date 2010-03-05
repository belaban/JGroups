// $Id: MessageSerializationTest.java,v 1.17 2010/03/05 09:05:55 belaban Exp $

package org.jgroups.tests;

/**
 * @author Filip Hanik
 * @author Bela Ban
 * @version 1.0
 */

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;


public class MessageSerializationTest {
    static final Log log=LogFactory.getLog(MessageSerializationTest.class);

    static final short UDP_ID=100;
    static final short PING_ID=101;
    static final short FD_SOCK_ID=102;
    static final short VERIFY_SUSPECT_ID=103;
    static final short STABLE_ID=104;
    static final short NAKACK_ID=105;
    static final short UNICAST_ID=106;
    static final short FRAG_ID=107;
    static final short GMS_ID=108;

    public MessageSerializationTest() {
    }


    public static void main(String[] args) throws Exception {
        boolean add_headers=false;
        InetAddress addr=InetAddress.getLocalHost();
        int num=10000;

        for(int i=0; i < args.length; i++) {
            if("-add_headers".equals(args[i])) {
                add_headers=true;
                continue;
            }
            if("-num".equals(args[i])) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }


        long start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            Message m=new Message(new IpAddress(addr, 5555), new IpAddress(addr, 6666), new byte[1000]);
            if(add_headers)
                addHeaders(m);

            ExposedByteArrayOutputStream msg_data=new ExposedByteArrayOutputStream();
            Buffer jgbuf;

            DataOutputStream dos=new DataOutputStream(msg_data);
            m.writeTo(dos);
            dos.close();

            jgbuf=new Buffer(msg_data.getRawBuffer(), 0, msg_data.size());

            ByteArrayInputStream msg_in_data=new ByteArrayInputStream(jgbuf.getBuf(), jgbuf.getOffset(), jgbuf.getLength());
            Message m2=(Message)Message.class.newInstance();

            DataInputStream dis=new DataInputStream(msg_in_data);
            m2.readFrom(dis);
            dis.close();
        }

        long stop=System.currentTimeMillis();
        System.out.println("Serializing and deserializing a message " + num + " times took " + (stop - start) + "ms.");
    }

    /**
     * Adds some dummy headers to the message
     */
     static void addHeaders(Message msg) {
        msg.putHeader(UDP_ID, new TpHeader("MyGroup"));
        msg.putHeader(PING_ID, new PingHeader(PingHeader.GET_MBRS_REQ, "demo-cluster"));
        msg.putHeader(FD_SOCK_ID, new FD_SOCK.FdHeader());
        msg.putHeader(VERIFY_SUSPECT_ID, new VERIFY_SUSPECT.VerifyHeader());
        msg.putHeader(STABLE_ID, new org.jgroups.protocols.pbcast.STABLE.StableHeader());
        msg.putHeader(NAKACK_ID, new org.jgroups.protocols.pbcast.NakAckHeader());
        msg.putHeader(UNICAST_ID, new UNICAST.UnicastHeader());
        msg.putHeader(FRAG_ID, new FragHeader());
        msg.putHeader(GMS_ID, new org.jgroups.protocols.pbcast.GMS.GmsHeader());
    }


    static void help() {
        System.out.println("MessageSerializationTest [-help] [-add_headers] [-num <iterations>]");
    }
}
