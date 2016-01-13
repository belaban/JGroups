
package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.util.Buffer;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.DataOutput;

/**
 * Tests the size of marshalled messages (multicast, unicast)
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageSizeTest {
    private static final byte MULTICAST=2;

    private static final short UDP_ID=100;
    private static final short UNICAST_ID=101;
    private static final short NAKACK_ID=102;


    private static final int MCAST_MAX_SIZE=84;
    private static final int UNICAST_MAX_SIZE=102;


    /**
     * Tests size of a multicast message.
     * Current record: 84 bytes (March 2010)
     * Prev: 166, 109, 103, 84
     * @throws Exception
     */
    public static void testMulticast() throws Exception {
        Address src=Util.createRandomAddress();
        Message msg=createMessage(null, src);
        Buffer buf=marshal(msg);
        System.out.println("buf = " + buf);

        int len=buf.getLength();
        System.out.println("len = " + len);

        assert len <= MCAST_MAX_SIZE;
        if(len < MCAST_MAX_SIZE) {
            double percentage=compute(len, MCAST_MAX_SIZE);
            System.out.println("multicast message (" + len + " bytes) is " + Util.format(percentage) +
                    "% smaller than previous max size (" + MCAST_MAX_SIZE + " bytes)");
        }
    }

    /**
     * Tests size of a unicast message.
     * Current record: 102 (March 2010)
     * Prev: 161, 127, 121, 102
     * @throws Exception
     */
    public static void testUnicast() throws Exception {
        Address dest=Util.createRandomAddress();
        Address src=Util.createRandomAddress();
        Message msg=createMessage(dest, src);
        Buffer buf=marshal(msg);
        System.out.println("buf = " + buf);

        int len=buf.getLength();
        System.out.println("len = " + len);

        assert len <= UNICAST_MAX_SIZE;
        if(len < UNICAST_MAX_SIZE) {
            double percentage=compute(len, UNICAST_MAX_SIZE);
            System.out.println("multicast message (" + len + " bytes) is " + Util.format(percentage) +
                    "% smaller than previous max size (" + UNICAST_MAX_SIZE + " bytes)");
        }
    }


    private static double compute(int new_length, int old_length) {
        if(new_length >= old_length)
            return 0.0;

        return 100.0* (1.0 - (new_length / (double)old_length));
    }

    private static Buffer marshal(Message msg) throws Exception {
        ByteArrayDataOutputStream dos=new ByteArrayDataOutputStream((int)(msg.size() + 50));
        Address dest=msg.getDest();
        boolean multicast=dest == null;
        writeMessage(msg, dos, multicast);
        return dos.getBuffer();
    }
    
    protected static void writeMessage(Message msg, DataOutput dos, boolean multicast) throws Exception {
        byte flags=0;
        dos.writeShort(Version.VERSION); // write the version
        if(multicast)
            flags+=MULTICAST;
        dos.writeByte(flags);
        msg.writeTo(dos);
    }


    static Message createMessage(Address dest, Address src) {
        Message msg=new Message(dest, src, "hello world");
        msg.putHeader(NAKACK_ID, NakAckHeader.createMessageHeader(322649));
        msg.putHeader(UNICAST_ID, UNICAST.UnicastHeader.createDataHeader(465784, (short)23323, true));
        msg.putHeader(UDP_ID, new TpHeader("DrawDemo"));
        return msg;
    }

}