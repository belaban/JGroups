// $Id: MessageSerializationTest.java,v 1.15 2008/03/31 06:15:21 vlada Exp $

package org.jgroups.tests;

/**
 * @author Filip Hanik
 * @author Bela Ban
 * @version 1.0
 */

import org.jgroups.Message;
import org.jgroups.util.ExposedByteArrayOutputStream;
import org.jgroups.util.Buffer;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.stack.IpAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.io.*;


public class MessageSerializationTest {
    static final Log log=LogFactory.getLog(MessageSerializationTest.class);

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
        msg.putHeader("UDP", new TpHeader("MyGroup"));
        msg.putHeader("PING", new PingHeader(PingHeader.GET_MBRS_REQ, "x"));
        msg.putHeader("FD_SOCK", new FD_SOCK.FdHeader());
        msg.putHeader("VERIFY_SUSPECT", new VERIFY_SUSPECT.VerifyHeader());
        msg.putHeader("STABLE", new org.jgroups.protocols.pbcast.STABLE.StableHeader());
        msg.putHeader("NAKACK", new org.jgroups.protocols.pbcast.NakAckHeader());
        msg.putHeader("UNICAST", new UNICAST.UnicastHeader());
        msg.putHeader("FRAG", new FragHeader());
        msg.putHeader("GMS", new org.jgroups.protocols.pbcast.GMS.GmsHeader());
    }


    static void help() {
        System.out.println("MessageSerializationTest [-help] [-add_headers] [-num <iterations>]");
    }
}
