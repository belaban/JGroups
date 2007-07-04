// $Id: StreamableTest.java,v 1.1 2007/07/04 07:29:33 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.PingRsp;
import org.jgroups.protocols.UdpHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Vector;


public class StreamableTest extends TestCase {
    Message m1, m2;


    public StreamableTest(String name) {
        super(name);
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

        m3=new Message(false);
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
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, new PingRsp(src, src, true));
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
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, new PingRsp(src, src, false));
        msg.putHeader("ping-header", hdr);
        UdpHeader udp_hdr=new UdpHeader("bla");
        msg.putHeader("udp-header", udp_hdr);
        stream(msg);
    }



    public void testMergeView() throws Exception {
        Vector tmp_m1, tmp_m2 , m3, all, subgroups;
        Address a,b,c,d,e,f;
        View v1, v2, v3, v4, v5, view_all;

        a=new IpAddress(1000);
        b=new IpAddress(2000);
        c=new IpAddress(3000);
        d=new IpAddress(4000);
        e=new IpAddress(5000);
        f=new IpAddress(6000);

        tmp_m1=new Vector(); tmp_m2=new Vector(); m3=new Vector(); all=new Vector(); subgroups=new Vector();
        tmp_m1.add(a); tmp_m1.add(b); tmp_m1.add(c);
        tmp_m2.add(d);
        m3.add(e); m3.add(f);
        all.add(a); all.add(b); all.add(c); all.add(d); all.add(e); all.add(f);

        v1=new View(a, 1, tmp_m1);
        v2=new MergeView(d, 2, tmp_m2, new Vector());
        v3=new View(e, 3, m3);
        v4=new MergeView(e, 4, m3, null);
        v5=new View(e, 5, m3);
        subgroups.add(v1);
        subgroups.add(v2);
        subgroups.add(v3);
        subgroups.add(v4);
        subgroups.add(v5);

        view_all=new MergeView(a, 5, all, subgroups);
        System.out.println("MergeView: " + view_all);
        Vector sub=((MergeView)view_all).getSubgroups();
        assertTrue(sub.get(0) instanceof View);
        assertTrue(sub.get(1) instanceof MergeView);
        assertTrue(sub.get(2) instanceof View);
        assertTrue(sub.get(3) instanceof MergeView);
        assertTrue(sub.get(4) instanceof View);

        byte[] buf=Util.streamableToByteBuffer(view_all);
        assertNotNull(buf);
        assertTrue(buf.length > 0);

        MergeView merge_view=(MergeView)Util.streamableFromByteBuffer(MergeView.class, buf);
        assertNotNull(merge_view);
        System.out.println("MergeView: " + merge_view);
        sub=merge_view.getSubgroups();
        assertTrue(sub.get(0) instanceof View);
        assertTrue(sub.get(1) instanceof MergeView);
        assertTrue(sub.get(2) instanceof View);
        assertTrue(sub.get(3) instanceof MergeView);
        assertTrue(sub.get(4) instanceof View);
    }

    private void stream(Message msg) throws Exception {
        int length, bufLength;
        byte[] tmp;
        Message msg2;
        Address src;
        int num_headers=getNumHeaders(msg);

        length=msg.getLength();
        bufLength=getBufLength(msg);
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
        // assertTrue(match(dest, msg2.getDest()));
        assertNull(msg2.getDest()); // we don't marshal the destination address
        assertTrue(match(src, msg2.getSrc()));
        assertEquals(num_headers, getNumHeaders(msg2));
    }

    private int getNumHeaders(Message msg) {
        return msg.getNumHeaders();
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
        return new TestSuite(StreamableTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
