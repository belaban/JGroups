
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.DataOutputStream;

/**
 * Tests the size of marshalled messages (multicast, unicast, individual messages or message lists)
 * @author Bela Ban
 * @version $Id: MessageSizeTest.java,v 1.1 2010/02/23 15:34:50 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageSizeTest {
    private static final byte MULTICAST=2;


    /**
     * Tests size of a multicast message.
     * Current record: 144 bytes
     * @throws Exception
     */
    public static void testMulticast() throws Exception {
        Address src=Util.createRandomAddress();
        Message msg=createMessage(null, src);
        Buffer buf=marshal(msg);
        System.out.println("buf = " + buf);

        int len=buf.getLength();
        System.out.println("len = " + len);
    }


    private static Buffer marshal(Message msg) throws Exception {
        ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream((int)(msg.size() + 50));
        ExposedDataOutputStream dos=new ExposedDataOutputStream(out_stream);
        Address dest=msg.getDest();
        boolean multicast=dest == null || dest.isMulticastAddress();
        writeMessage(msg, dos, multicast);
        return new Buffer(out_stream.getRawBuffer(), 0, out_stream.size());
    }
    
    protected static void writeMessage(Message msg, DataOutputStream dos, boolean multicast) throws Exception {
        byte flags=0;
        dos.writeShort(Version.version); // write the version
        if(multicast)
            flags+=MULTICAST;
        dos.writeByte(flags);
        msg.writeTo(dos);
    }


    static Message createMessage(Address dest, Address src) {
        Message msg=new Message(dest, src, "hello world");
        Address sender=Util.createRandomAddress();
        msg.putHeader("NAKACK", new NakAckHeader(NakAckHeader.MSG, 322649, 450000, sender));
        msg.putHeader("UNICAST", new UNICAST.UnicastHeader(UNICAST.UnicastHeader.DATA, 465784, 23323, true));
        msg.putHeader("UDP", new TpHeader("DrawDemo"));
        return msg;
    }

}