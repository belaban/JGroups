
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.auth.*;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;
import org.jgroups.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;


/**
 * Tests whether method size() of a header and its serialized size correspond
 * @author  Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class SizeTest {


    public static void testTpHeader() throws Exception {
        _testSize(new TpHeader("DemoChannel"));
    }


    public static void testPingHeader() throws Exception {
        _testSize(new PingHeader(PingHeader.GET_MBRS_REQ, "bla"));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, new PingData()));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, (PingData)null));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, (String)null));
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, new PingData(Util.createRandomAddress(), null, true)));
        Address self=Util.createRandomAddress();
        PingData rsp=new PingData(self, Util.createView(self, 1, self), true);
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, rsp));
        rsp=new PingData(self, Util.createView(self, 1, self, Util.createRandomAddress(), Util.createRandomAddress()), true);
        _testSize(new PingHeader(PingHeader.GET_MBRS_RSP, rsp));
    }
 

    public static void testPingData() throws Exception {
        PingData data;
        final Address own=org.jgroups.util.UUID.randomUUID();
        final Address coord=org.jgroups.util.UUID.randomUUID();
        final PhysicalAddress physical_addr_1=new IpAddress("127.0.0.1", 7500);
        final PhysicalAddress physical_addr_2=new IpAddress("192.168.1.5", 6000);
        final PhysicalAddress physical_addr_3=new IpAddress("192.134.2.1", 6655);
        final Address self=Util.createRandomAddress();

        data=new PingData(null, null, false);
        _testSize(data);

        data=new PingData(own, Util.createView(coord, 22, coord, Util.createRandomAddress()), false);
        _testSize(data);

        data=new PingData(null, null, false, "node-1", null);
        _testSize(data);

        data=new PingData(own, Util.createView(coord, 22, coord), false, "node-1", null);
        _testSize(data);

        data=new PingData(own, Util.createView(coord, 22, coord), false, "node-1", new ArrayList<PhysicalAddress>(7));
        _testSize(data);

        data=new PingData(null, null, false, "node-1", new ArrayList<PhysicalAddress>(7));
        _testSize(data);

        List<PhysicalAddress> list=new ArrayList<PhysicalAddress>();
        list.add(physical_addr_1);
        list.add(physical_addr_2);
        list.add(physical_addr_3);
        data=new PingData(null, null, false, "node-1", list);
        _testSize(data);

        list.clear();
        list.add(new IpAddress("127.0.0.1", 7500));
        data=new PingData(null, null, false, "node-1", list);
        _testSize(data);

        View view=Util.createView(coord, 322649, coord, own, UUID.randomUUID());
        data.setView(view);
        _testSize(data);

         data=new PingData(self, Util.createView(self, 1, self), true, "logical-name", null);
        _testSize(data);
    }

    public static void testAuthHeader() throws Exception {
        _testSize(new AuthHeader(new SimpleToken("secret")));
        _testSize(new AuthHeader(new FixedMembershipToken("192.168.1.5[7800],192.168.1.3[7800]")));
        _testSize(new AuthHeader(new MD5Token("myauthvalue")));
        _testSize(new AuthHeader(new RegexMembership()));

        X509Token tok=new X509Token().encryptedToken(new byte[]{'b', 'e', 'l', 'a'});
        _testSize(new AuthHeader(tok));
    }

    public static void testGossipData() throws Exception {
        GossipData data;
        final Address own=org.jgroups.util.UUID.randomUUID();
        final Address coord=org.jgroups.util.UUID.randomUUID();
        UUID.add(own, "own");
        UUID.add(coord, "coord");

        final PhysicalAddress physical_addr_1=new IpAddress("127.0.0.1", 7500);
        final PhysicalAddress physical_addr_2=new IpAddress("192.168.1.5", 6000);
        final PhysicalAddress physical_addr_3=new IpAddress("192.134.2.1", 6655);

        _testSize(new GossipData());

        data=new GossipData((byte)1);
        _testSize(data);

        data=new GossipData((byte)1, "DemoCluster", own, (List<Address>)null, null);
        _testSize(data);

        data=new GossipData((byte)1, "DemoCluster", own, Arrays.asList(own, coord), null);
        _testSize(data);

        data=new GossipData((byte)1, "DemoCluster", own, Arrays.asList(own, coord),
                            Arrays.asList(physical_addr_1, physical_addr_2, physical_addr_3));
        _testSize(data);

        List<PhysicalAddress> list=new ArrayList<PhysicalAddress>();
        list.add(physical_addr_1);
        list.add(physical_addr_2);
        list.add(physical_addr_3);
        data=new GossipData((byte)1, "DemoCluster", own, Arrays.asList(own, coord), list); 
        _testSize(data);

        data=new GossipData((byte)1, "demo", own, "logical_name", null);
        _testSize(data);

        data=new GossipData((byte)1, "demo", own, new byte[]{'b', 'e', 'l', 'a'});
        _testSize(data);

        byte[] buffer=new byte[10];
        buffer[2]='B';
        buffer[3]='e';
        buffer[4]='l';
        buffer[5]='a';
        data=new GossipData((byte)1, "demo", own, buffer, 2, 4);
        _testSize(data);
    }


    public static void testDigest() throws Exception {
        Address addr=Util.createRandomAddress();
        MutableDigest mutableDigest=new MutableDigest(2);
        mutableDigest.add(addr, 200, 205);
        mutableDigest.add(Util.createRandomAddress(), 104, 105);
        _testSize(mutableDigest);

        Digest digest=new MutableDigest(10);
        _testSize(digest);

        digest=new Digest(Util.createRandomAddress(), 45, 50);
        _testSize(digest);
    }

    public static void testNakackHeader() throws Exception {
        _testSize(NakAckHeader.createMessageHeader(322649));
        _testSize(NakAckHeader.createXmitRequestHeader(100, 104, Util.createRandomAddress()));
        _testSize(NakAckHeader.createXmitResponseHeader());
    }


    public static void testFdHeaders() throws Exception {
        FD.FdHeader hdr=new FD.FdHeader(FD.FdHeader.HEARTBEAT_ACK);
        _testSize(hdr);

        IpAddress a1=new IpAddress("127.0.0.1", 5555);
        IpAddress a2=new IpAddress("127.0.0.1", 6666);
        List<Address> suspects=new ArrayList<Address>();
        suspects.add(a1);
        suspects.add(a2);
        hdr=new FD.FdHeader(FD.FdHeader.SUSPECT, suspects, a1);
        _testSize(hdr);

        FD_SOCK.FdHeader sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE);
        _testSize(sockhdr);

        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, new IpAddress("127.0.0.1", 5555));
        _testSize(sockhdr);

        Set<Address> tmp=new HashSet<Address>();
        tmp.add(a1);
        tmp.add(a2);
        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, tmp);
        _testSize(sockhdr);


        Map<Address,IpAddress> cache=new HashMap<Address,IpAddress>();
        cache.put(a1, a2);
        cache.put(a2, a1);
        sockhdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT, cache);
        _testSize(sockhdr);
    }



    public static void testFdSockHeaders() throws Exception {
        FD_SOCK.FdHeader hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE);
        _testSize(hdr);

        hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE, new IpAddress("127.0.0.1", 4567));
        _testSize(hdr);

        Set<Address> set=new HashSet<Address>();
        set.add(new IpAddress(3452));
        set.add(new IpAddress("127.0.0.1", 5000));

        hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE, set);
        _testSize(hdr);

        Map<Address,IpAddress> map=new HashMap<Address,IpAddress>();
        map.put(new IpAddress("127.0.0.1", 5000), new IpAddress(4553));
        map.put(new IpAddress("127.0.0.1", 6000), new IpAddress(4523));
        map.put(new IpAddress(7000), new IpAddress(4553));
         hdr=new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE, map);
        _testSize(hdr);
        
        // check that IpAddress is correctly sized in FD_SOCK.FdHeader
        hdr = new FD_SOCK.FdHeader(FD_SOCK.FdHeader.I_HAVE_SOCK, new IpAddress("127.0.0.1", 4567), 
				   new IpAddress("127.0.0.1", 4567));
        _testSize(hdr) ;
    }




    public static void testUnicastHeader() throws Exception {
        UNICAST.UnicastHeader hdr=UNICAST.UnicastHeader.createDataHeader(322649, (short)127, false);
        _testSize(hdr);

        hdr=UNICAST.UnicastHeader.createDataHeader(322649, Short.MAX_VALUE, false);
        _testSize(hdr);

        hdr=UNICAST.UnicastHeader.createDataHeader(322649, (short)(Short.MAX_VALUE -10), true);
        _testSize(hdr);

        hdr=UNICAST.UnicastHeader.createAckHeader(322649);
        _testSize(hdr);

        hdr=UNICAST.UnicastHeader.createSendFirstSeqnoHeader(33333);
        _testSize(hdr);
    }

    public static void testUnicast2Header() throws Exception {
        UNICAST2.Unicast2Header hdr=UNICAST2.Unicast2Header.createDataHeader(322649, (short)127, false);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createDataHeader(322649, Short.MAX_VALUE, false);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createDataHeader(322649, (short)(Short.MAX_VALUE -10), true);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createSendFirstSeqnoHeader(322649);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createStableHeader((short)55, 0, 0);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createStableHeader((short)55, 0, 1);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createStableHeader((short)55, 70000, 100000);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createStableHeader((short)55, Integer.MAX_VALUE, (long)Integer.MAX_VALUE+1000);
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createXmitReqHeader();
        _testSize(hdr);
        _testMarshalling(hdr);

        hdr=UNICAST2.Unicast2Header.createSendFirstSeqnoHeader(322649);
        _testSize(hdr);
        _testMarshalling(hdr);
    }


    public static void testStableHeader() throws Exception {
        org.jgroups.protocols.pbcast.STABLE.StableHeader hdr;
        Address addr=UUID.randomUUID();
        Map<Address,long[]> map=new HashMap<Address,long[]>();
        map.put(addr, new long[]{200, 205});
        Digest digest=new Digest(map);
        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABLE_GOSSIP, digest);
        _testSize(hdr);

        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABILITY, null);
        _testSize(hdr);
    }


    public static void testStableHeader2() throws Exception {
        org.jgroups.protocols.pbcast.STABLE.StableHeader hdr;
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        MutableDigest digest=new MutableDigest(2);
        digest.add(addr, 200, 205);
        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABLE_GOSSIP, digest);
        _testSize(hdr);

        hdr=new STABLE.StableHeader(STABLE.StableHeader.STABILITY, null);
        _testSize(hdr);
    }


    public static void testSequencerHeader() throws Exception {
        org.jgroups.protocols.SEQUENCER.SequencerHeader hdr;
        hdr=new SEQUENCER.SequencerHeader((byte)1, 1L);
        _testSize(hdr);
    }


    public static void testAddressVector() throws Exception {
        List<Address> v=new ArrayList<Address>();
        _testSize(v);
        v.add(new IpAddress(1111));
        _testSize(v);
        v.add(new IpAddress(2222));
        _testSize(v);
    }


    public static void testViewId() throws Exception {
        ViewId vid=new ViewId();
        _testSize(vid);

        Address addr=Util.createRandomAddress("A");

        vid=new ViewId(addr);
        _testSize(vid);

        vid=new ViewId(addr, 322649);
        _testSize(vid);
    }


    public static void testMergeId() throws Exception {
        MergeId id=MergeId.create(UUID.randomUUID());
        System.out.println("id = " + id);
        _testSize(id);

        id=MergeId.create(UUID.randomUUID());
        System.out.println("id = " + id);
        _testSize(id);

        Address addr=UUID.randomUUID();
        id=MergeId.create(addr);
        System.out.println("id = " + id);
        _testSize(id);

        id=MergeId.create(addr);
        System.out.println("id = " + id);
        _testSize(id);

        id=MergeId.create(addr);
        System.out.println("id = " + id);
        _testSize(id);
    }


    public static void testView() throws Exception {
        Address one=Util.createRandomAddress("A");
        ViewId vid=new ViewId(one, 322649);
        List<Address> mbrs=new ArrayList<Address>();
        mbrs.add(one);
        View v=new View(vid, mbrs);
        _testSize(v);
        
        mbrs.add(Util.createRandomAddress("B"));
        v=new View(vid, mbrs);
        _testSize(v);

        mbrs.add(Util.createRandomAddress("C"));
        v=new View(vid, mbrs);
        _testSize(v);
    }


    public static void testMergeView() throws Exception {
        ViewId vid=new ViewId(UUID.randomUUID(), 322649);
        List<Address> mbrs=new ArrayList<Address>();
        View v=new MergeView(vid, mbrs, null);
        _testSize(v);
        mbrs.add(UUID.randomUUID());
        _testSize(v);
        mbrs.add(UUID.randomUUID());
        _testSize(v);
    }



    public static void testMergeView2() throws Exception {
        List<Address> m1, m2 , m3, all;
        List<View> subgroups;
        Address a,b,c,d,e,f;
        View v1, v2, v3, view_all;

        a=Util.createRandomAddress();
        b=Util.createRandomAddress();
        c=Util.createRandomAddress();
        d=Util.createRandomAddress();
        e=Util.createRandomAddress();
        f=Util.createRandomAddress();

        m1=new ArrayList<Address>(); m2=new ArrayList<Address>(); m3=new ArrayList<Address>(); all=new ArrayList<Address>();
        subgroups=new ArrayList<View>();
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



    public static void testMergeView3() throws Exception {
        List<Address> m1, m2 , m3, all;
        List<View> subgroups;
        Address a,b,c,d,e,f;
        View v1, v2, v3, v4, view_all;

        a=new IpAddress(1000);
        b=new IpAddress(2000);
        c=new IpAddress(3000);
        d=new IpAddress(4000);
        e=new IpAddress(5000);
        f=new IpAddress(6000);

        m1=new ArrayList<Address>(); m2=new ArrayList<Address>(); m3=new ArrayList<Address>(); all=new ArrayList<Address>();
        subgroups=new ArrayList<View>();
        m1.add(a); m1.add(b); m1.add(c);
        m2.add(d);
        m3.add(e); m3.add(f);
        all.add(a); all.add(b); all.add(c); all.add(d); all.add(e); all.add(f);

        v1=new View(a, 1, m1);
        v2=new MergeView(d, 2, m2, new ArrayList<View>());
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


    public static void testMergeHeader() throws Exception {
        MERGE3.MergeHeader hdr=new MERGE3.MergeHeader();
        _testSize(hdr);
        ViewId view_id=new ViewId(Util.createRandomAddress("A"), 22);
        hdr=MERGE3.MergeHeader.createInfo(view_id, null, null);
        _testSize(hdr);
        String logical_name="A";
        hdr=MERGE3.MergeHeader.createInfo(view_id, logical_name, null);
        _testSize(hdr);
        List<PhysicalAddress> physical_addr=new ArrayList<PhysicalAddress>();
        physical_addr.add(new IpAddress(5002));
        hdr=MERGE3.MergeHeader.createInfo(view_id, logical_name, physical_addr);
        _testSize(hdr);
        Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"), c=Util.createRandomAddress("C");
        View view=Util.createView(a, 22, a,b,c);
        hdr=MERGE3.MergeHeader.createViewRequest();
        _testSize(hdr);
        hdr=MERGE3.MergeHeader.createViewResponse(view);
        _testSize(hdr);
    }


    public static void testJoinRsp() throws Exception {
        JoinRsp rsp;
        Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"), c=Util.createRandomAddress("C");
        View v=Util.createView(a, 55, a, b, c);

        MutableDigest digest=new MutableDigest(3);
        digest.add(a, 1000, 1050);
        digest.add(b, 700, 700);
        digest.add(c, 0, 0);
        rsp=new JoinRsp(v, digest);
        _testSize(rsp);

        rsp=new JoinRsp(v, null);
        _testSize(rsp);

        rsp=new JoinRsp("boom");
        _testSize(rsp);
    }

    public static void testLargeJoinRsp() throws Exception {
        int NUM=1000;
        Address[] members=new Address[NUM];
        for(int i=0; i < members.length; i++)
            members[i]=Util.createRandomAddress("m" + i);

        View view=Util.createView(members[0], 53, members);
        MutableDigest digest=new MutableDigest(NUM);
        for(Address member: members)
            digest.add(member, 70000, 100000, false);

        JoinRsp rsp=new JoinRsp(view, digest);
        _testSize(rsp);

        rsp=new JoinRsp(view, null);
        _testSize(rsp);
    }


    public static void testGmsHeader() throws Exception {
        Address addr=UUID.randomUUID();
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, addr);
        _testSize(hdr);

        List<Address> members=new ArrayList<Address>();
        members.add(addr);
        members.add(addr);
        View v=new View(addr, 33, members);
        hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, v);
        _testSize(hdr);

        Collection<Address> mbrs=new ArrayList<Address>();
        Collections.addAll(mbrs, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ, mbrs);
    }


    public static void testFCHeader() throws Exception {
        FcHeader hdr=new FcHeader(FcHeader.REPLENISH);
        _testSize(hdr);
    }


    public static void testFragHeader() throws Exception {
        FragHeader hdr=new FragHeader(322649, 1, 10);
        _testSize(hdr);
    }


    public static void testCompressHeader() throws Exception {
        COMPRESS.CompressHeader hdr=new COMPRESS.CompressHeader(2002);
        _testSize(hdr);
    }


    public static void testStompHeader() throws Exception {
        STOMP.StompHeader hdr=STOMP.StompHeader.createHeader(STOMP.StompHeader.Type.MESSAGE,
                                                             "destination", "/topics/chat",
                                                             "sender", UUID.randomUUID().toString());
        _testSize(hdr);

        hdr=STOMP.StompHeader.createHeader(STOMP.StompHeader.Type.ENDPOINT, "endpoint", "192.168.1.5:8787");
        _testSize(hdr);
    }

    public static void testRelayHeader() throws Exception {
        RELAY.RelayHeader hdr=RELAY.RelayHeader.create(RELAY.RelayHeader.Type.FORWARD);
        _testSize(hdr);

        hdr=RELAY.RelayHeader.createDisseminateHeader(Util.createRandomAddress("A"));
        _testSize(hdr);

        Map<Address,String> uuid_cache=new HashMap<Address,String>();
        uuid_cache.put(Util.createRandomAddress("A"), "A");
        uuid_cache.put(Util.createRandomAddress("B"), "B");
        uuid_cache.put(Util.createRandomAddress("B"), "B");
        // hdr=RELAY.RelayHeader.create(RELAY.RelayHeader.Type.UUIDS);
        // _testSize(hdr);
    }

    public static void testStateHeader() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        STATE_TRANSFER.StateHeader hdr;
        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_REQ, null);
        _testSize(hdr);

        MutableDigest digest=new MutableDigest(2);
        digest.add(addr, 200, 205);
        digest.add(new IpAddress(2314), 104, 105);
        hdr=new STATE_TRANSFER.StateHeader(STATE_TRANSFER.StateHeader.STATE_RSP, digest);
        _testSize(hdr);
    }


    public static void testRelay2Header() throws Exception {
        Address dest=new SiteMaster((short)0);
        RELAY2.Relay2Header hdr=new RELAY2.Relay2Header(RELAY2.Relay2Header.DATA, dest, null);
        _testSize(hdr);
        Address sender=new SiteUUID(UUID.randomUUID(), "dummy", (short)1);
        hdr=new RELAY2.Relay2Header(RELAY2.Relay2Header.DATA, dest, sender);
        _testSize(hdr);
    }


    public static void testEncryptHeader() throws Exception {
        ENCRYPT.EncryptHeader hdr=new ENCRYPT.EncryptHeader((short)1, null);
        _testSize(hdr);
        hdr=new ENCRYPT.EncryptHeader((short)2, "Hello world");
        _testSize(hdr);
    }


    public static void testIpAddress() throws Exception {
        IpAddress addr=new IpAddress();
        _testSize(addr);
    }


    public static void testIpAddress1() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 5555);
        _testSize(addr);
    }


    public static void testIpAddressWithHighPort() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 65535);
        _testSize(addr);
    }


    public static void testIpAddress2() throws Exception {
        IpAddress addr=new IpAddress(3456);
        _testSize(addr);
    }


    public static void testIpAddress3() throws Exception {
        IpAddress addr=new IpAddress(5555, false);
        _testSize(addr);
    }





    public static void testWriteAddress() throws Exception {
        Address uuid=UUID.randomUUID();
        _testWriteAddress(uuid);

        Address addr=new IpAddress(7500);
        _testWriteAddress(addr);

        addr=new IpAddress("127.0.0.1", 5678);
        _testWriteAddress(addr);
    }

    private static void _testWriteAddress(Address addr) throws Exception {
        int len=Util.size(addr);
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        Util.writeAddress(addr, out);
        out.flush();
        byte[] buf=output.toByteArray();
        out.close();

        System.out.println("\nlen=" + len + ", serialized length=" + buf.length);
        assert len == buf.length;
        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buf));
        Address new_addr=Util.readAddress(in);
        System.out.println("old addr=" + addr + "\nnew addr=" + new_addr);
        assert addr.equals(new_addr);
    }



    public static void testWriteAddresses() throws Exception {
        List<Address> list=new ArrayList<Address>();
        for(int i=0; i < 3; i++)
            list.add(UUID.randomUUID());
        _testWriteAddresses(list);

        list.clear();
        list.add(new IpAddress(7500));
        list.add(new IpAddress("192.168.1.5", 4444));
        list.add(new IpAddress("127.0.0.1", 5674));
        _testWriteAddresses(list);
    }

    private static void _testWriteAddresses(List<Address> list) throws Exception {
        long len=Util.size(list);
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        Util.writeAddresses(list, out);
        out.flush();
        byte[] buf=output.toByteArray();
        out.close();

        System.out.println("\nlen=" + len + ", serialized length=" + buf.length);
        assert len == buf.length;
        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buf));
        Collection<? extends Address> new_list=Util.readAddresses(in, ArrayList.class);
        System.out.println("old list=" + list + "\nnew list=" + new_list);
        assert list.equals(new_list);
    }



    public static void testUUID() throws Exception {
        org.jgroups.util.UUID uuid=org.jgroups.util.UUID.randomUUID();
        System.out.println("uuid = " + uuid);
        _testSize(uuid);

        uuid=org.jgroups.util.UUID.randomUUID();
        byte[] buf=Util.streamableToByteBuffer(uuid);
        org.jgroups.util.UUID uuid2=(org.jgroups.util.UUID)Util.streamableFromByteBuffer(org.jgroups.util.UUID.class, buf);
        System.out.println("uuid:  " + uuid);
        System.out.println("uuid2: " + uuid2);
        assert uuid.equals(uuid2);

        int hash1=uuid.hashCode(), hash2=uuid2.hashCode();
        System.out.println("hash 1: " + hash1);
        System.out.println("hash 2: " + hash2);
        assert hash1 == hash2;
    }



    public static void testRequestCorrelatorHeader() throws Exception {
        RequestCorrelator.Header hdr;

        hdr=new RequestCorrelator.Header(RequestCorrelator.Header.REQ, 322649, false, (short)1000);
        _testSize(hdr);

        hdr=new RequestCorrelator.Header(RequestCorrelator.Header.RSP, 322649, true, (short)356);

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

        Assert.assertEquals(322649, hdr.id);
        assert hdr.rsp_expected;
        Assert.assertEquals((short)356, hdr.corrId);
        Assert.assertEquals(RequestCorrelator.Header.RSP, hdr.type);


        hdr=new RequestCorrelator.Header(RequestCorrelator.Header.RSP, 322649, true, (short)356);

        output=new ByteArrayOutputStream();
        out=new DataOutputStream(output);
        hdr.writeTo(out);
        out.flush();

        buf=output.toByteArray();
        out.close();

        input=new ByteArrayInputStream(buf);
        in=new DataInputStream(input);

        hdr=new RequestCorrelator.Header();
        hdr.readFrom(in);

        Assert.assertEquals(322649, hdr.id);
        assert hdr.rsp_expected;
        Assert.assertEquals(356, hdr.corrId);
        Assert.assertEquals(RequestCorrelator.Header.RSP, hdr.type);
    }


    private static void _testMarshalling(UNICAST2.Unicast2Header hdr) throws Exception {
        byte[] buf=Util.streamableToByteBuffer(hdr);
        UNICAST2.Unicast2Header hdr2=(UNICAST2.Unicast2Header)Util.streamableFromByteBuffer(UNICAST2.Unicast2Header.class, buf);

        assert hdr.getType()      == hdr2.getType();
        assert hdr.getSeqno()     == hdr2.getSeqno();
        assert hdr.getHighSeqno() == hdr2.getHighSeqno();
        assert hdr.getConnId()    == hdr2.getConnId();
        assert hdr.isFirst()      == hdr2.isFirst();
    }

    private static void _testSize(Digest digest) throws Exception {
        long len=digest.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(digest);
        System.out.println("digest = " + digest);
        System.out.println("size=" + len + ", serialized size=" + serialized_form.length);
        assert len == serialized_form.length;
    }

    private static void _testSize(Header hdr) throws Exception {
        long size=hdr.size();
        byte[] serialized_form=Util.streamableToByteBuffer(hdr);
        System.out.println(hdr.getClass().getSimpleName() + ": size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);

        Header hdr2=(Header)Util.streamableFromByteBuffer(hdr.getClass(), serialized_form);
        assert hdr2.size() == hdr.size();
    }


    private static void _testSize(Address addr) throws Exception {
        long size=addr.size();
        byte[] serialized_form=Util.streamableToByteBuffer(addr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);
    }


    private static void _testSize(ViewId vid) throws Exception {
        long size=vid.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(vid);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);
    }

    private static void _testSize(MergeId id) throws Exception {
        long size=id.size();
        byte[] serialized_form=Util.streamableToByteBuffer(id);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assert serialized_form.length == size;
    }

    private static void _testSize(View v) throws Exception {
        long size=v.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(v);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);
    }

    private static void _testSize(Collection<Address> coll) throws Exception {
        long size=Util.size(coll);
        byte[] serialized_form=Util.collectionToByteBuffer(coll);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);
    }

    private static void _testSize(MERGE3.MergeHeader hdr) throws Exception {
        long size=hdr.size();
        byte[] serialized_form=Util.streamableToByteBuffer(hdr);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);
    }

    private static void _testSize(JoinRsp rsp) throws Exception {
        long size=rsp.serializedSize();
        byte[] serialized_form=Util.streamableToByteBuffer(rsp);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(serialized_form.length, size);

        JoinRsp rsp2=(JoinRsp)Util.streamableFromByteBuffer(JoinRsp.class, serialized_form);
        assert Util.match(rsp.getDigest(), rsp2.getDigest());
        assert Util.match(rsp.getView(), rsp2.getView());
        assert Util.match(rsp.getFailReason(), rsp2.getFailReason());
    }


    private static void _testSize(SizeStreamable data) throws Exception {
        System.out.println("\ndata: " + data);
        long size=data.size();
        byte[] serialized_form=Util.streamableToByteBuffer(data);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        assert serialized_form.length == size : "serialized length=" + serialized_form.length + ", size=" + size;
    }


}
