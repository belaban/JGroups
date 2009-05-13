// $Id: MessageListStreamableTest.java,v 1.7 2009/05/13 13:06:58 belaban Exp $

package org.jgroups.tests;

/**
 * @author Bela Ban
 * @version $Id: MessageListStreamableTest.java,v 1.7 2009/05/13 13:06:58 belaban Exp $
 */

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;


public class MessageListStreamableTest {
    static final Log log=LogFactory.getLog(MessageListStreamableTest.class);

    public MessageListStreamableTest() {
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

        Buffer buf;
        List<Message> list=new LinkedList<Message>();
        long start=System.currentTimeMillis();
        long stop;
        for(int i=0; i < num; i++) {
            Message m=new Message(new IpAddress(addr, 5555), new IpAddress(addr, 6666), new byte[256]);
            if(add_headers)
                addHeaders(m);
            list.add(m);
        }

        start=System.currentTimeMillis();
        buf=Util.msgListToByteBuffer(list);
        stop=System.currentTimeMillis();
        System.out.println("Marshalling a message list of " + list.size() + " elements took " + (stop - start) + "ms.");

        start=System.currentTimeMillis();
        List<Message> list2=Util.byteBufferToMessageList(buf.getBuf(), buf.getOffset(), buf.getLength());
        stop=System.currentTimeMillis();
        System.out.println("Unmarshalling a message list of " + list2.size() + " elements took " + (stop - start) + "ms.");
    }

    /**
     * Adds some dummy headers to the message
     */
    static void addHeaders(Message msg) {
        msg.putHeader("UDP", new TpHeader("MyGroup"));
        msg.putHeader("PING", new PingHeader(PingHeader.GET_MBRS_REQ, "demo-cluster"));
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
