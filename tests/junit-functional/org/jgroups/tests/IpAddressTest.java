
package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class IpAddressTest {
    IpAddress a, b, c, d, e, f, g, h;

    
    @BeforeClass
    void setUp() throws Exception {
        StackType type=Util.getIpStackType();
        if(type == StackType.IPv6) {
            a=new IpAddress("::1", 5555);
            b=new IpAddress("::1", 5555);
            d=new IpAddress("::1", 5556);
            e=new IpAddress("::1", 5555);
            f=new IpAddress("2001:0db8:0000:0000:0000:002e:0370:2334", 80);
            g=new IpAddress("2001:0db8:0000:0000:0000:002e:0370:2334", 8080);
            h=new IpAddress("ff0e::3:4:5", 5555);
        }
        else {
            a=new IpAddress("localhost", 5555);
            b=new IpAddress("localhost", 5555);
            d=new IpAddress("localhost", 5556);
            e=new IpAddress("127.0.0.1", 5555);
            f=new IpAddress("www.ibm.com", 80);
            g=new IpAddress("www.ibm.com", 8080);
            h=new IpAddress("224.0.0.1", 5555);
        }
        c=b;
    }

    public void testConstructor() throws Exception {
        IpAddress tmp=new IpAddress("192.168.1.5:7800");
        assert tmp.getPort() == 7800;
        assert tmp.getIpAddress().equals(InetAddress.getByName("192.168.1.5"));

        tmp=new IpAddress("10.1.2.3");
        assert tmp.getPort() == 0;
        assert tmp.getIpAddress().equals(InetAddress.getByName("10.1.2.3"));

        tmp=new IpAddress("fe80::21b:21ff:fe07:a3b0:6000");
        assert tmp.getPort() == 6000;
        assert tmp.getIpAddress().equals(InetAddress.getByName("fe80::21b:21ff:fe07:a3b0"));
    }

    public void testUnknownAddress() {
        try {
            IpAddress tmp=new IpAddress("idontknow.com", 55);
            assert false : "should throw an UnknownHostException for " + tmp;
        }
        catch(UnknownHostException e1) {
        }
    }

    public void testCopy() {
        IpAddress tmp=a.copy();
        assert tmp.equals(a);
    }

    public void testEquality() throws Exception {
        Assert.assertEquals(a, b);
        Assert.assertEquals(c, b);
        Assert.assertEquals(a, e);
        Assert.assertEquals(c, e);
    }


    public static void testEqualityWithDnsRoundRobin() throws UnknownHostException {
        IpAddress x1, x2, x3;

        StackType type=Util.getIpStackType();
        String tmp=type == StackType.IPv6? "::1" : "127.0.0.1";
        InetAddress addr=InetAddress.getByName(tmp);
        byte[] rawAddr=addr.getAddress();

        InetAddress inet1=InetAddress.getByAddress("MyHost1", rawAddr);
        InetAddress inet2=InetAddress.getByAddress("MyHost2", rawAddr);
        InetAddress inet3=InetAddress.getByAddress("MyHost3", rawAddr);
        Assert.assertEquals(inet1, inet2);

        x1=new IpAddress(inet1, 5555);
        x2=new IpAddress(inet2, 5555);
        x3=new IpAddress(inet3, 5555);

        Assert.assertEquals(x1, x2);
        Assert.assertEquals(x3, x1);

        Set<Address> s=new HashSet<>();
        Collections.addAll(s, x1, x2, x3);
        System.out.println("s=" + s);
        Assert.assertEquals(1, s.size());

        Map<Address,String> m=new HashMap<>();
        m.put(x1, "Bela");
        m.put(x2, "Michelle");
        m.put(x3, "Nicole");
        Assert.assertEquals(1, m.size());
        Assert.assertEquals("Nicole", m.get(x1));
    }



    public void testInequality() throws Exception {
        IpAddress tmp=null;
        assert !a.equals(d);
        assert !c.equals(d);
        assert !a.equals(f);
        assert !e.equals(f);
        assert !f.equals(g);
        assert !(a.equals(tmp));
    }



    public void testSameHost() throws Exception {
        assert Util.sameHost(a, b);
        assert Util.sameHost(a, c);
        assert Util.sameHost(a, d);
        assert Util.sameHost(a, e);
        assert Util.sameHost(f, g);
    }



    public void testNotSameHost() throws Exception {
        assert !Util.sameHost(a, f);
        assert !Util.sameHost(e, f);
        assert !Util.sameHost(e, null);
        assert !Util.sameHost(null, null);
    }



    public void testCompareTo() throws UnknownHostException {
        Assert.assertEquals(0, a.compareTo(b));
        assert a.compareTo(d) < 0;
        assert d.compareTo(a) > 0;
    }



    public void testCompareTime() {
        final int NUM=1000000;
        _testCompareTime(a, a, NUM);
        _testCompareTime(a, b, NUM);
        _testCompareTime(a, c, NUM);
        _testCompareTime(a, d, NUM);
    }


    private static void _testCompareTime(IpAddress one, IpAddress two, int num) {
        int rc=-99;
        long start=System.currentTimeMillis(), stop;
        for(int x=0; x < num; x++) {
            rc=one.compareTo(two);
        }
        stop=System.currentTimeMillis();
        long diff=stop-start;
        System.out.printf("calling compareTo(%s, %s) %d times took %d ms, result=%d\n", one, two, num, diff, rc);
    }



    public void testHashcode() {
        int hcode_a=a.hashCode();
        int hcode_b=b.hashCode();
        Assert.assertEquals(hcode_a, hcode_b);

        Map<Address,Integer> map=new HashMap<>();
        map.put(a, 1);
        map.put(d, 2);
        System.out.println("map = " + map);
        assert map.size() == 2;

        map.put(a, 22);
        assert map.size() == 2;
        Integer val=map.get(a);
        assert val != null && val == 22;

        val=map.get(a);
        assert val != null && val == 22;

        map.put(a, 50);
        assert map.size() == 2;
        val=map.get(a);
        assert val != null && val == 50;
    }



    public void testHashcodeTime() {
        int hash=-1;
        final int NUM=10000000;

        long start=System.currentTimeMillis(), stop;
        for(int x=0; x < NUM; x++) {
            hash=a.hashCode();
        }
        stop=System.currentTimeMillis();
        long diff=stop-start;
        System.out.println("taking the hash code of " + a + "(" + hash + ") took " + diff + "ms");
    }



    public static void testIPv6WithStreamable() throws Exception {
        IpAddress ip=new IpAddress("fe80:0:0:0:21b:21ff:fe07:a3b0", 5555);

        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      dos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       dis;

        System.out.println("-- address is " + ip);

        ip.writeTo(dos);
        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        dis=new DataInputStream(bis);
        IpAddress ip2=new IpAddress();
        ip2.readFrom(dis);
        Assert.assertEquals(ip, ip2);
    }




    public void testStreamable() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      oos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       ois;
        IpAddress             a2, b2, x, x2, y, y2;

        x=createStackConformantAddress(5555);

        y=createStackConformantAddress(1111);

        a.writeTo(oos);
        b.writeTo(oos);
        x.writeTo(oos);
        y.writeTo(oos);

        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new DataInputStream(bis);
        a2=new IpAddress();
        a2.readFrom(ois);
        b2=new IpAddress();
        b2.readFrom(ois);
        x2=new IpAddress();
        x2.readFrom(ois);
        y2=new IpAddress();
        y2.readFrom(ois);

        Assert.assertEquals(a, a2);
        Assert.assertEquals(b, b2);

        assert y2.getIpAddress() != null;
        Assert.assertEquals(1111, y2.getPort());
    }




    public static void testStreamableWithHighPort() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      oos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       dis;
        IpAddress             x, x2;

        x=createStackConformantAddress(65535);
        x.writeTo(oos);

        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        dis=new DataInputStream(bis);

        x2=new IpAddress();
        x2.readFrom(dis);
        System.out.println("x: " + x + ", x2: " + x2);

        assert x2.getPort() > 0;
        Assert.assertEquals(x.getPort(), x2.getPort());
    }




    public void testStreamableAdditionalData() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      oos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       ois;
        IpAddress             a2, b2, c2, d2, e2, f2, g2, h2;

        a.writeTo(oos);
        b.writeTo(oos);
        c.writeTo(oos);
        d.writeTo(oos);
        e.writeTo(oos);
        f.writeTo(oos);
        g.writeTo(oos);
        h.writeTo(oos);


        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new DataInputStream(bis);
        a2=new IpAddress();
        a2.readFrom(ois);
        b2=new IpAddress();
        b2.readFrom(ois);
        c2=new IpAddress();
        c2.readFrom(ois);
        d2=new IpAddress();
        d2.readFrom(ois);
        e2=new IpAddress();
        e2.readFrom(ois);
        f2=new IpAddress();
        f2.readFrom(ois);
        g2=new IpAddress();
        g2.readFrom(ois);
        h2=new IpAddress();
        h2.readFrom(ois);

        Assert.assertEquals(b2, c2);
        Assert.assertEquals(a, a2);
        Assert.assertEquals(b, b2);
        Assert.assertEquals(c, c2);
        Assert.assertEquals(d, d2);
        Assert.assertEquals(e, e2);
        Assert.assertEquals(f, f2);
        Assert.assertEquals(g, g2);
        Assert.assertEquals(h, h2);
    }


    private static IpAddress createStackConformantAddress(int port) throws UnknownHostException {
        StackType type=Util.getIpStackType();
        if(type == StackType.IPv6)
            return new IpAddress("::1", port);
        else
            return new IpAddress("127.0.0.1", port);
    }


}
