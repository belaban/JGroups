// $Id: MessageSerializationTest.java,v 1.9 2005/04/13 13:04:11 belaban Exp $

package org.jgroups.tests;

/**
 * @author Filip Hanik
 * @author Bela Ban
 * @version 1.0
 */

import org.jgroups.Message;
import org.jgroups.util.MagicObjectOutputStream;
import org.jgroups.util.MagicObjectInputStream;
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
        boolean use_magic=false;
        boolean use_streamable=false;

        for(int i=0; i < args.length; i++) {
            if("-add_headers".equals(args[i])) {
                add_headers=true;
                continue;
            }
            if("-num".equals(args[i])) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if("-use_magic".equals(args[i])) {
                use_magic=true;
                continue;
            }
            if("-use_streamable".equals(args[i])) {
                use_streamable=true;
                continue;
            }
            help();
            return;
        }


        ClassConfigurator.getInstance(true);
        long start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            Message m=new Message(new IpAddress(addr, 5555), new IpAddress(addr, 6666), new byte[256]);
            if(add_headers)
                addHeaders(m);

            ExposedByteArrayOutputStream msg_data=new ExposedByteArrayOutputStream();
            Buffer jgbuf;

            if(use_streamable) {
                DataOutputStream dos=new DataOutputStream(msg_data);
                m.writeTo(dos);
                dos.close();
            }
            else {
                ObjectOutputStream msg_out=use_magic? new MagicObjectOutputStream(msg_data) : new ObjectOutputStream(msg_data);
                m.writeExternal(msg_out);
                // msg_out.writeObject(m);
                msg_out.close();
            }

            jgbuf=new Buffer(msg_data.getRawBuffer(), 0, msg_data.size());

            ByteArrayInputStream msg_in_data=new ByteArrayInputStream(jgbuf.getBuf(), jgbuf.getOffset(), jgbuf.getLength());
            Message m2=(Message)Message.class.newInstance();

            if(use_streamable) {
                DataInputStream dis=new DataInputStream(msg_in_data);
                m2.readFrom(dis);
                dis.close();
            }
            else {
                ObjectInputStream msg_in=use_magic? new MagicObjectInputStream(msg_in_data) : new ObjectInputStream(msg_in_data);

                m2.readExternal(msg_in);
                // Message m2=(Message)msg_in.readObject();
                msg_in.close();
            }

        }

        long stop=System.currentTimeMillis();
        System.out.println("Serializing and deserializing a message " + num + " times took " + (stop - start) + "ms.");
    }

    /**
     * Adds some dummy headers to the message
     */
    static void addHeaders(Message msg) {
        msg.putHeader("UDP", new UdpHeader("MyGroup"));
        msg.putHeader("PING", new PingHeader(PingHeader.GET_MBRS_REQ, null));
        msg.putHeader("FD_SOCK", new FD_SOCK.FdHeader());
        msg.putHeader("VERIFY_SUSPECT", new VERIFY_SUSPECT.VerifyHeader());
        msg.putHeader("STABLE", new org.jgroups.protocols.pbcast.STABLE.StableHeader());
        msg.putHeader("NAKACK", new org.jgroups.protocols.pbcast.NakAckHeader());
        msg.putHeader("UNICAST", new UNICAST.UnicastHeader());
        msg.putHeader("FRAG", new FragHeader());
        msg.putHeader("GMS", new org.jgroups.protocols.pbcast.GMS.GmsHeader());
    }


    static void help() {
        System.out.println("MessageSerializationTest [-help] [-add_headers] [-num <iterations>] " +
                           "[-use_magic] [-use_streamable]");
    }
}
