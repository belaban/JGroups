// $Id: StreamableTest.java,v 1.1 2004/10/04 20:40:11 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.Address;
import org.jgroups.ChannelException;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.UdpHeader;
import org.jgroups.protocols.WanPipeAddress;
import org.jgroups.protocols.PingRsp;
import org.jgroups.stack.IpAddress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;



public class StreamableTest extends TestCase {
    Message m1, m2;


    public StreamableTest(String name) {
        super(name);
    }


    public void setUp() {

    }

    public void tearDown() {
        
    }


    static {
        try {
            ClassConfigurator.getInstance(true);
        }
        catch(ChannelException e) {
            e.printStackTrace();
        }
    }



    public void testStreamable() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        byte[] tmp;
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);


        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        m1.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        DataInputStream in=new DataInputStream(input);
        Message m3, m4;

        m3=new Message();
        m3.readFrom(in);

        assertEquals(4, m3.getLength());
        assertEquals(4, m3.getRawBuffer().length);
        assertEquals(4, m3.getBuffer().length);
        assertEquals(0, m3.getOffset());

        output=new ByteArrayOutputStream();
        out=new DataOutputStream(output);
        // out.writeObject(m2);
        m2.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        System.out.println("-- serialized buffer is " + tmp.length + " bytes");

        input=new ByteArrayInputStream(tmp);
        in=new DataInputStream(input);

        // m4=(Message)in.readObject();
        m4=new Message();
        m4.readFrom(in);


        assertEquals(3, m4.getLength());
        assertEquals(3, m4.getBuffer().length);
        assertEquals(3, m4.getRawBuffer().length);
        assertEquals(0, m4.getOffset());
    }



    public void testStreamable2() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message msg=new Message(null, null, buf, 0, 4);
        stream(msg);
    }

    public void testStreamable3() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message msg=new Message(null, null, buf, 4, 3);
        stream(msg);
    }

    public void testNullBuffer() throws Exception {
        Message msg=new Message();
        stream(msg);
    }


    public void testNonNullBuffer() throws Exception {
        Message msg=new Message(null, null, "Hello world".getBytes());
        stream(msg);
    }


    public void testNonNullAddress() throws Exception {
        Address dest, src;
        dest=new IpAddress("228.1.2.3", 5555);
        src=new IpAddress("127.0.0.1", 6666);
        Message msg=new Message(dest, src, "Hello world".getBytes());
        stream(msg);
    }

    public void testHeaders() throws Exception {
        Address dest, src;
        dest=new IpAddress("228.1.2.3", 5555);
        src=new IpAddress("127.0.0.1", 6666);
        Message msg=new Message(dest, src, "Hello world".getBytes());
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, new PingRsp(src, src));
        msg.putHeader("ping-header", hdr);
        UdpHeader udp_hdr=new UdpHeader("bla");
        msg.putHeader("udp-header", udp_hdr);
        stream(msg);
    }


    public void testAdditionalData() throws Exception {
        IpAddress dest, src;
        dest=new IpAddress("228.1.2.3", 5555);
        dest.setAdditionalData("foo".getBytes());
        src=new IpAddress("127.0.0.1", 6666);
        src.setAdditionalData("foobar".getBytes());
        Message msg=new Message(dest, src, "Hello world".getBytes());
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, new PingRsp(src, src));
        msg.putHeader("ping-header", hdr);
        UdpHeader udp_hdr=new UdpHeader("bla");
        msg.putHeader("udp-header", udp_hdr);
        stream(msg);
    }

    public void testDifferentAddress() throws Exception {
        Address dest, src;
        dest=new WanPipeAddress("foo");
        src=new WanPipeAddress("foobar");
        Message msg=new Message(dest, src, "Hello world".getBytes());
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, new PingRsp(src, src));
        msg.putHeader("ping-header", hdr);
        UdpHeader udp_hdr=new UdpHeader("bla");
        msg.putHeader("udp-header", udp_hdr);
        stream(msg);
    }

    private void stream(Message msg) throws Exception {
        int length, bufLength;
        byte[] tmp;
        Message msg2;
        Address dest, src;
        int num_headers=getNumHeaders(msg);

        length=msg.getLength();
        bufLength=getBufLength(msg);
        dest=msg.getDest();
        src=msg.getSrc();

        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        msg.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        System.out.println("-- serialized buffer is " + tmp.length + " bytes");

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        DataInputStream in=new DataInputStream(input);

        msg2=new Message();
        msg2.readFrom(in);

        assertEquals(length, msg2.getLength());
        assertEquals(bufLength, getBufLength(msg2));
        assertTrue(match(dest, msg2.getDest()));
        assertTrue(match(src, msg2.getSrc()));
        assertEquals(num_headers, getNumHeaders(msg2));
    }

    private int getNumHeaders(Message msg) {
        return msg.getHeaders() != null? msg.getHeaders().size() : 0;
    }


    private boolean match(Address a1, Address a2) {
        if(a1 == null && a2 == null)
            return true;
        if(a1 != null)
            return a1.equals(a2);
        else
            return a2.equals(a1);
    }

//    private int getRawBufLength(Message msg) {
//        return msg.getRawBuffer() != null? msg.getRawBuffer().length : 0;
//    }

    private int getBufLength(Message msg) {
        return msg.getBuffer() != null? msg.getBuffer().length : 0;
    }


    public static Test suite() {
        TestSuite s=new TestSuite(StreamableTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
