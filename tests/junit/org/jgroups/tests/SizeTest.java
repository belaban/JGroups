// $Id: SizeTest.java,v 1.5 2005/07/22 15:37:41 belaban Exp $$

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.Digest;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;

import java.util.*;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;


/**
 * Tests whether method size() of a header and its serialized size correspond
 */
public class SizeTest extends TestCase {


    public SizeTest(String name) {
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



    public void testUdpHeader(Header hdr) throws Exception {
        _testSize(new UdpHeader("DemoChannel"));
    }

    public void testTpHeader() throws Exception {
        _testSize(new TpHeader("DemoChannel"));
    }

    public void testPingHeader() throws Exception {
        _testSize(new PingHeader(PingHeader.GET_MBRS_REQ, null));
        IpAddress self=new IpAddress("127.0.0.1", 5555);
        PingRsp rsp=new PingRsp(self, self, true);
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, rsp));
    }

    public void testNakackHeader() throws Exception {
        _testSize(new NakAckHeader(NakAckHeader.MSG, 322649));
        _testSize(new NakAckHeader(NakAckHeader.XMIT_REQ, 100, 104, new IpAddress("127.0.0.1", 5655)));
        _testSize(new NakAckHeader(NakAckHeader.XMIT_RSP, 322649));
    }

    public void testFdHeaders() throws Exception {
        FD.FdHeader hdr=new FD.FdHeader(FD.FdHeader.HEARTBEAT_ACK);
        _testSize(hdr);

        IpAddress a1=new IpAddress("127.0.0.1", 5555);
        IpAddress a2=new IpAddress("127.0.0.1", 6666);
        Vector suspects=new Vector();
        suspects.add(a1);
        suspects.add(a2);
        hdr=new FD.FdHeader(FD.FdHeader.SUSPECT, suspects, a1);
        _testSize(hdr);

        FD_SOCK.FdHeader sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE);
        _testSize(sockhdr);

        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, new IpAddress("127.0.0.1", 5555));
        _testSize(sockhdr);

        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, suspects);
        _testSize(sockhdr);


        Hashtable cache=new Hashtable();
        cache.put(a1, a2);
        cache.put(a2, a1);
        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, cache);
        _testSize(sockhdr);
    }



    public void testUnicastHeader() throws Exception {
        UNICAST.UnicastHeader hdr=new UNICAST.UnicastHeader(UNICAST.UnicastHeader.DATA, 322649);
        _testSize(hdr);
    }

    public void testStableHeader() throws Exception {
        org.jgroups.protocols.pbcast.STABLE.StableHeader hdr;
        Digest digest=new Digest(2);
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        digest.add(addr, 100, 200, 205);
        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABLE_GOSSIP, digest);
        _testSize(hdr);

        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABILITY, null);
        _testSize(hdr);
    }

    public void testAddressVector() throws Exception {
        Vector v=new Vector();
        _testSize(v);
        v.add(new IpAddress(1111));
        _testSize(v);
        v.add(new IpAddress(2222));
        _testSize(v);
    }

    public void testViewId() throws Exception {
        ViewId vid=new ViewId();
        _testSize(vid);

        vid=new ViewId(new IpAddress(5555));
        _testSize(vid);

        vid=new ViewId(new IpAddress(5555), 322649);
        _testSize(vid);
    }

    public void testView() throws Exception {
        View v=new View();
        _testSize(v);

        ViewId vid=new ViewId(new IpAddress(1111), 322649);
        Vector mbrs=new Vector();
        v=new View(vid, mbrs);
        _testSize(v);
        mbrs.add(new IpAddress(3333));
        _testSize(v);
        mbrs.add(new IpAddress(1111));
        _testSize(v);
    }

    public void testJoinRsp() throws Exception {
        JoinRsp rsp;
        Vector members=new Vector();

        members.add(new IpAddress(1111));
        members.add(new IpAddress(2222));
        View v=new View(new IpAddress(1234), 322649, members);
        Digest d=new Digest(3);
        d.add(new IpAddress(3524), 1,2,3);
        d.add(new IpAddress(1324), 3,4,5);
        rsp=new JoinRsp();
        _testSize(rsp);
        rsp=new JoinRsp(v, d);
        _testSize(rsp);
    }

    public void testGmsHeader() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, addr);
        _testSize(hdr);

        Vector members=new Vector();
        members.add(addr);
        members.add(addr);
        View v=new View(addr, 33, members);
        hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, v);
        _testSize(hdr);

    }

    public void testFCHeader() throws Exception {
        FC.FcHeader hdr=new FC.FcHeader(FC.FcHeader.REPLENISH);
        _testSize(hdr);
    }

    public void testFragHeader() throws Exception {
        FragHeader hdr=new FragHeader(322649, 1, 10);
        _testSize(hdr);
    }

    public void testCompressHeader() throws Exception {
        COMPRESS.CompressHeader hdr=new COMPRESS.CompressHeader(2002);
        _testSize(hdr);
    }


    public void testStateHeader() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        STATE_TRANSFER.StateHeader hdr;
        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_REQ, addr, 322649, null);
        _testSize(hdr);
        Digest digest=new Digest(2);
        digest.add(addr, 100, 200, 205);
        digest.add(new IpAddress(2314), 102, 104, 105);
        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_RSP, addr, 322649, digest);
        _testSize(hdr);
    }

    public void testIpAddress() throws Exception {
        IpAddress addr=new IpAddress();
        _testSize(addr);
    }

    public void testIpAddress1() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        _testSize(addr);
    }

    public void testIpAddress2() throws Exception {
        IpAddress addr=new IpAddress(3456);
        _testSize(addr);
    }

    public void testIpAddress3() throws Exception {
        IpAddress addr=new IpAddress(5555, false);
        _testSize(addr);
    }

    public void testIpAddressWithAdditionalData() throws Exception {
        IpAddress addr=new IpAddress(5555, false);
        addr.setAdditionalData("bela".getBytes());
        _testSize(addr);
    }


    public void testRequestCorrelatorHeader() throws Exception {
        RequestCorrelator.Header hdr;

        hdr=new RequestCorrelator.Header(RequestCorrelator.Header.REQ, 322649, false, "HelloWorld");
        _testSize(hdr);

        hdr=new RequestCorrelator.Header(RequestCorrelator.Header.RSP, 322649, true, "bla");
        java.util.List l=new LinkedList();
        l.add(new IpAddress(1111));
        l.add(new IpAddress(2222));
        hdr.dest_mbrs=l;
        hdr.callStack=new Stack();
        hdr.callStack.push(new IpAddress(2222));
        hdr.callStack.push(new IpAddress(3333));
        _testSize(hdr);

        hdr=new RequestCorrelator.Header(RequestCorrelator.Header.RSP, 322649, true, "bla");
        hdr.callStack=new Stack();
        hdr.callStack.push(new IpAddress(2222));
        hdr.callStack.push(new IpAddress(3333));

        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        hdr.writeTo(out);
        out.flush();

        byte[] buf=output.toByteArray();
        out.close();

        ByteArrayInputStream input=new ByteArrayInputStream(buf);
        DataInputStream in=new DataInputStream(input);

        hdr=new RequestCorrelator.Header();
        hdr.readFrom(in);
        System.out.println("call stack is " + hdr.callStack);

        Address tmp=(Address)hdr.callStack.pop();
        assertEquals(tmp, new IpAddress(3333));
        tmp=(Address)hdr.callStack.pop();
        assertEquals(tmp, new IpAddress(2222));
        assertEquals(hdr.id, 322649);
        assertTrue(hdr.rsp_expected);
        assertEquals(hdr.corrName, "bla");
        assertEquals(hdr.type, RequestCorrelator.Header.RSP);
    }

    private void _testSize(Header hdr) throws Exception {
        long size=hdr.size();
        byte[] serialized_form=Util.streamableToByteBuffer((Streamable)hdr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }


     private void _testSize(Address addr) throws Exception {
        long size=addr.size();
        byte[] serialized_form=Util.streamableToByteBuffer(addr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }


    private void _testSize(ViewId vid) throws Exception {
        long size=vid.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(vid);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private void _testSize(View v) throws Exception {
        long size=v.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(v);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private void _testSize(Collection coll) throws Exception {
        long size=Util.size(coll);
        byte[] serialized_form=Util.collectionToByteBuffer(coll);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private void _testSize(JoinRsp rsp) throws Exception {
        long size=rsp.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(rsp);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }



    public static Test suite() {
        TestSuite s=new TestSuite(SizeTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
