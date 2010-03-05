// $Id: MessageListStreamableTest.java,v 1.8 2010/03/05 09:05:55 belaban Exp $

package org.jgroups.tests;

/**
 * @author Bela Ban
 * @version $Id: MessageListStreamableTest.java,v 1.8 2010/03/05 09:05:55 belaban Exp $
 */

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;


public class MessageListStreamableTest {
    static final Log log=LogFactory.getLog(MessageListStreamableTest.class);

    static final short UDP_ID=100;
    static final short PING_ID=101;
    static final short FD_SOCK_ID=102;
    static final short VERIFY_SUSPECT_ID=103;
    static final short STABLE_ID=104;
    static final short NAKACK_ID=105;
    static final short UNICAST_ID=106;
    static final short FRAG_ID=107;
    static final short GMS_ID=108;


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
        System.out.println("MessageSerializationTest [-help] [-add_headers] [-num <iterations>] " +
                           "[-use_magic] [-use_streamable]");
    }
}
