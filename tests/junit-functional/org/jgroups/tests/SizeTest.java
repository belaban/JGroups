// $Id: SizeTest.java,v 1.4 2007/11/29 11:18:54 belaban Exp $$

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.mux.ServiceInfo;
import org.jgroups.mux.MuxHeader;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Digest;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;
import org.jgroups.util.MutableDigest;

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



    public void testUdpHeader() throws Exception {
        _testSize(new UdpHeader("DemoChannel"));
    }

    public void testTpHeader() throws Exception {
        _testSize(new TpHeader("DemoChannel"));
    }

    public void testPingHeader() throws Exception {
        _testSize(new PingHeader(PingHeader.GET_MBRS_REQ, "bla"));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, new PingRsp()));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, (PingRsp)null));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, (String)null));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, new PingRsp(new IpAddress(4444), null, true)));
        IpAddress self=new IpAddress("127.0.0.1", 5555);
        PingRsp rsp=new PingRsp(self, self, true);
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, rsp));
    }

    public void testNakackHeader() throws Exception {
        _testSize(new NakAckHeader(NakAckHeader.MSG, 322649));
        _testSize(new NakAckHeader(NakAckHeader.XMIT_REQ, 100, 104, new IpAddress("127.0.0.1", 5655)));
        _testSize(new NakAckHeader(NakAckHeader.XMIT_RSP, 100, 104, new IpAddress("127.0.0.1", 5655)));
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

        Set tmp=new HashSet();
        tmp.add(a1);
        tmp.add(a2);
        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, tmp);
        _testSize(sockhdr);


        Hashtable cache=new Hashtable();
        cache.put(a1, a2);
        cache.put(a2, a1);
        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, cache);
        _testSize(sockhdr);
    }


    public void testFdSockHeaders() throws Exception {
        FD_SOCK.FdHeader hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE);
        _testSize(hdr);

        hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE, new IpAddress("127.0.0.1", 4567));
        _testSize(hdr);

        Set<Address> set=new HashSet<Address>();
        set.add(new IpAddress(3452));
        set.add(new IpAddress("127.0.0.1", 5000));

        hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE, set);
        _testSize(hdr);

        Hashtable<Address,IpAddress> map=new Hashtable<Address,IpAddress>();
        map.put(new IpAddress("127.0.0.1", 5000), new IpAddress(4553));
        map.put(new IpAddress("127.0.0.1", 6000), new IpAddress(4523));
        map.put(new IpAddress(7000), new IpAddress(4553));
         hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE, map);
        _testSize(hdr);
    }



    public void testUnicastHeader() throws Exception {
        UNICAST.UnicastHeader hdr=new UNICAST.UnicastHeader(UNICAST.UnicastHeader.DATA, 322649);
        _testSize(hdr);
    }

    public void testStableHeader() throws Exception {
        org.jgroups.protocols.pbcast.STABLE.StableHeader hdr;
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        Map map=new HashMap();
        map.put(addr, new Digest.Entry(100, 200, 205));
        Digest digest=new Digest(map);
        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABLE_GOSSIP, digest);
        _testSize(hdr);

        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABILITY, null);
        _testSize(hdr);
    }

    public void testStableHeader2() throws Exception {
        org.jgroups.protocols.pbcast.STABLE.StableHeader hdr;
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        MutableDigest digest=new MutableDigest(2);
        digest.add(addr, 100, 200, 205);
        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABLE_GOSSIP, digest);
        _testSize(hdr);

        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABILITY, null);
        _testSize(hdr);
    }

    public void testSequencerHeader() throws Exception {
        org.jgroups.protocols.SEQUENCER.SequencerHeader hdr;
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        hdr=new SEQUENCER.SequencerHeader((byte)1, addr, 1L);
        _testSize(hdr);
        hdr=new SEQUENCER.SequencerHeader((byte)2, null, -1L);
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

    public void testViewPayload() throws Exception {
        View v=new View();
        v.addPayload("name", "Bela Ban");
        _testSize(v);

        ViewId vid=new ViewId(new IpAddress(1111), 322649);
        Vector mbrs=new Vector();
        v=new View(vid, mbrs);
        v.addPayload("id", 322649);
        v.addPayload("name", "Michelle");
        _testSize(v);
        mbrs.add(new IpAddress(3333));
        _testSize(v);
        mbrs.add(new IpAddress(1111));
        _testSize(v);
    }

    public void testMergeView() throws Exception {
        View v=new MergeView();
        _testSize(v);

        ViewId vid=new ViewId(new IpAddress(1111), 322649);
        Vector mbrs=new Vector();
        v=new MergeView(vid, mbrs, null);
        _testSize(v);
        mbrs.add(new IpAddress(3333));
        _testSize(v);
        mbrs.add(new IpAddress(1111));
        _testSize(v);
    }


    public void testMergeView2() throws Exception {
        Vector m1, m2 , m3, all, subgroups;
        Address a,b,c,d,e,f;
        View v1, v2, v3, view_all;

        a=new IpAddress(1000);
        b=new IpAddress(2000);
        c=new IpAddress(3000);
        d=new IpAddress(4000);
        e=new IpAddress(5000);
        f=new IpAddress(6000);

        m1=new Vector(); m2=new Vector(); m3=new Vector(); all=new Vector(); subgroups=new Vector();
        m1.add(a); m1.add(b); m1.add(c);
        m2.add(d);
        m3.add(e); m3.add(f);
        all.add(a); all.add(b); all.add(c); all.add(d); all.add(e); all.add(f);

        v1=new View(a, 1, m1);
        v2=new View(d, 2, m2);
        v3=new View(e, 3, m3);
        subgroups.add(v1);
        subgroups.add(v2);
        subgroups.add(v3);

        view_all=new MergeView(a, 5, all, subgroups);
        System.out.println("MergeView: " + view_all);
        _testSize(view_all);
    }


    public void testMergeView3() throws Exception {
        Vector m1, m2 , m3, all, subgroups;
        Address a,b,c,d,e,f;
        View v1, v2, v3, v4, view_all;

        a=new IpAddress(1000);
        b=new IpAddress(2000);
        c=new IpAddress(3000);
        d=new IpAddress(4000);
        e=new IpAddress(5000);
        f=new IpAddress(6000);

        m1=new Vector(); m2=new Vector(); m3=new Vector(); all=new Vector(); subgroups=new Vector();
        m1.add(a); m1.add(b); m1.add(c);
        m2.add(d);
        m3.add(e); m3.add(f);
        all.add(a); all.add(b); all.add(c); all.add(d); all.add(e); all.add(f);

        v1=new View(a, 1, m1);
        v2=new MergeView(d, 2, m2, new Vector());
        v3=new View(e, 3, m3);
        v4=new MergeView(e, 4, m3, null);
        subgroups.add(v1);
        subgroups.add(v2);
        subgroups.add(v3);
        subgroups.add(v4);

        view_all=new MergeView(a, 5, all, subgroups);
        System.out.println("MergeView: " + view_all);
        _testSize(view_all);
    }


    public void testViewSyncHeader() throws Exception {
        Address creator=new IpAddress("localhost", 12345);
        Vector members=new Vector();
        members.add(new IpAddress(5555));
        members.add(creator);
        View view=new View(creator, 322649, members);
        VIEW_SYNC.ViewSyncHeader hdr=new VIEW_SYNC.ViewSyncHeader(VIEW_SYNC.ViewSyncHeader.VIEW_SYNC, view);
        _testSize(hdr);

        view=new MergeView();
        hdr=new VIEW_SYNC.ViewSyncHeader(VIEW_SYNC.ViewSyncHeader.VIEW_SYNC, view);
        _testSize(hdr);

        Vector subgroups=new Vector();
        subgroups.add(view);
        view=new MergeView(creator, 322649, members, subgroups);
        hdr=new VIEW_SYNC.ViewSyncHeader(VIEW_SYNC.ViewSyncHeader.VIEW_SYNC, view);
        _testSize(hdr);
    }


    public void testJoinRsp() throws Exception {
        JoinRsp rsp;
        Vector members=new Vector();

        members.add(new IpAddress(1111));
        members.add(new IpAddress(2222));
        View v=new View(new IpAddress(1234), 322649, members);
        MutableDigest d=new MutableDigest(3);
        d.add(new IpAddress(3524), 1,2,3);
        d.add(new IpAddress(1324), 3,4,5);
        rsp=new JoinRsp();
        _testSize(rsp);
        rsp=new JoinRsp(v, d);
        _testSize(rsp);
        rsp=new JoinRsp("this is a failure");
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

        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_REQ, addr, 322649, null, "my_state");
        _testSize(hdr);


        MutableDigest digest=new MutableDigest(2);
        digest.add(addr, 100, 200, 205);
        digest.add(new IpAddress(2314), 102, 104, 105);
        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_RSP, addr, 322649, digest);
        _testSize(hdr);

        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_RSP, addr, 322649, digest, "my_state");
        _testSize(hdr);
    }


    public void testEncryptHeader() throws Exception {
        ENCRYPT.EncryptHeader hdr=new ENCRYPT.EncryptHeader((short)1, null);
        _testSize(hdr);
        hdr=new ENCRYPT.EncryptHeader((short)2, "Hello world");
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

    public void testIpAddressWithHighPort() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 65535);
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

        Address tmp=hdr.callStack.pop();
        assertEquals(tmp, new IpAddress(3333));
        tmp=hdr.callStack.pop();
        assertEquals(tmp, new IpAddress(2222));
        assertEquals(322649, hdr.id);
        assertTrue(hdr.rsp_expected);
        assertEquals("bla", hdr.corrName);
        assertEquals(RequestCorrelator.Header.RSP, hdr.type);
    }


    public void testServiceInfo() throws Exception {
        ServiceInfo si=new ServiceInfo();
        _testSize(si);     
    }


    public void testMuxHeader() throws Exception {
        MuxHeader hdr=new MuxHeader();
        _testSize(hdr);

        hdr=new MuxHeader("bla");
        _testSize(hdr);

        ServiceInfo si=new ServiceInfo();
        hdr=new MuxHeader(si);
        _testSize(hdr);       
        _testSize(new MuxHeader(si));
    }


    private static void _testSize(Header hdr) throws Exception {
        long size=hdr.size();
        byte[] serialized_form=Util.streamableToByteBuffer((Streamable)hdr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }


    private static void _testSize(VIEW_SYNC.ViewSyncHeader hdr) throws Exception {
        long size=hdr.size();
        byte[] serialized_form=Util.streamableToByteBuffer(hdr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);

        VIEW_SYNC.ViewSyncHeader hdr2=(VIEW_SYNC.ViewSyncHeader)Util.streamableFromByteBuffer(VIEW_SYNC.ViewSyncHeader.class, serialized_form);

        int my_type=hdr.getType(), other_type=hdr2.getType();
        View my_view=hdr.getView(), other_view=hdr2.getView();
        System.out.println("my_type=" + my_type + ", other_type=" + other_type);
        System.out.println("my_view=" + my_view + ", other_view=" + other_view);
        assertEquals(my_type, other_type);
        assertEquals(my_view, other_view);
    }


     private static void _testSize(Address addr) throws Exception {
        long size=addr.size();
        byte[] serialized_form=Util.streamableToByteBuffer(addr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }


    private static void _testSize(ViewId vid) throws Exception {
        long size=vid.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(vid);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private static void _testSize(View v) throws Exception {
        long size=v.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(v);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private static void _testSize(Collection coll) throws Exception {
        long size=Util.size(coll);
        byte[] serialized_form=Util.collectionToByteBuffer(coll);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private static void _testSize(JoinRsp rsp) throws Exception {
        long size=rsp.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(rsp);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }

    private static void _testSize(ServiceInfo si) throws Exception {
        long size=si.size();
        byte[] serialized_form=Util.streamableToByteBuffer(si);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assertEquals(serialized_form.length, size);
    }


    private static void _testSize(MuxHeader hdr) throws Exception {
         long size=hdr.size();
         byte[] serialized_form=Util.streamableToByteBuffer(hdr);
         System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
         assertEquals(serialized_form.length, size);
     }



    public static Test suite() {
        return new TestSuite(SizeTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
