
package org.jgroups.tests;

import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.Global;
import org.jgroups.Address;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;

@Test(groups=Global.FUNCTIONAL,sequential=true)
public class IpAddressTest {
    IpAddress a, b, c, d, e, f, g, h, i, j, k;

    
    @BeforeMethod
    public void setUp() throws Exception {
        a=new IpAddress("localhost", 5555);
        b=new IpAddress("localhost", 5555);
        c=b;
        d=new IpAddress("localhost", 5556);
        e=new IpAddress("127.0.0.1", 5555);
        f=new IpAddress("www.ibm.com", 80);
        g=new IpAddress("www.ibm.com", 8080);
        h=new IpAddress("224.0.0.1", 5555);
    }


    public static void testUnknownAddress() {
        try {
            IpAddress tmp=new IpAddress("idontknow.com", 55);
            assert false : "should throw an UnknownHostException for " + tmp;
        }
        catch(UnknownHostException e1) {
        }
    }

    public void testEquality() throws Exception {
        Assert.assertEquals(a, b);
        Assert.assertEquals(c, b);
        Assert.assertEquals(a, e);
        Assert.assertEquals(c, e);
    }


    public static void testEqualityWithDnsRoundRobin() throws UnknownHostException {
        IpAddress x1, x2, x3;

        InetAddress addr=InetAddress.getByName("127.0.0.1");
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

        HashSet<Address> s=new HashSet<Address>();
        s.add(x1);
        s.add(x2);
        s.add(x3);
        System.out.println("s=" + s);
        Assert.assertEquals(1, s.size());

        HashMap<Address,String> m=new HashMap<Address,String>();
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


    public void testMcast() {
        assert h.isMulticastAddress();
        assert !a.isMulticastAddress();
        assert !e.isMulticastAddress();
        assert !g.isMulticastAddress();

    }

    

    public void testCompareTo() {
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
        System.out.println("calling compareTo(" + one + ", " + two + ") " + num + " times took " +
                           diff + "ms, result=" + rc);
    }



    public void testHashcode() {
        int hcode_a=a.hashCode();
        int hcode_b=b.hashCode();
        Assert.assertEquals(hcode_a, hcode_b);
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



    public static void testIPv6WithExternalization() throws IOException, ClassNotFoundException {
        InetAddress tmp=Util.getFirstNonLoopbackIPv6Address();
        IpAddress ip=new IpAddress(tmp, 5555);

        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        ObjectOutputStream    oos=new ObjectOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        ObjectInputStream     ois;

        System.out.println("-- address is " + tmp);

        oos.writeObject(ip);
        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new ObjectInputStream(bis);
        IpAddress ip2=(IpAddress)ois.readObject();
        Assert.assertEquals(ip, ip2);
    }




    public static void testIPv6WithStreamable() throws IOException, ClassNotFoundException {
        InetAddress tmp=Util.getFirstNonLoopbackIPv6Address();
        IpAddress ip=new IpAddress(tmp, 5555);

        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      dos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       dis;

        System.out.println("-- address is " + tmp);

        ip.writeTo(dos);
        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        dis=new DataInputStream(bis);
        IpAddress ip2=new IpAddress();
        ip2.readFrom(dis);
        Assert.assertEquals(ip, ip2);
    }


    public void testExternalization() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        ObjectOutputStream    oos=new ObjectOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        ObjectInputStream     ois;
        IpAddress             a2, b2;
        
        a.setAdditionalData(null);
        b.setAdditionalData("Bela Ban".getBytes());
        oos.writeObject(a);
        oos.writeObject(b);
        

        buf=bos.toByteArray();        
        bis=new ByteArrayInputStream(buf);
        ois=new ObjectInputStream(bis);
        a2=(IpAddress)ois.readObject();
        b2=(IpAddress)ois.readObject();

        Assert.assertEquals(a, a2);
        Assert.assertEquals(b, b2);

        assert a2.getAdditionalData() == null;
        Assert.assertEquals("Bela Ban", new String(b2.getAdditionalData()));
    }

    
    

    public void testExternalizationAdditionalData() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        ObjectOutputStream    oos=new ObjectOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        ObjectInputStream     ois;
        IpAddress             a2, b2, c2, d2, e2, f2, g2, h2;
        
        oos.writeObject(a);
        oos.writeObject(b);
        oos.writeObject(c);
        oos.writeObject(d);
        oos.writeObject(e);
        oos.writeObject(f);
        oos.writeObject(g);
        oos.writeObject(h);


        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new ObjectInputStream(bis);
        a2=(IpAddress)ois.readObject();
        b2=(IpAddress)ois.readObject();
        c2=(IpAddress)ois.readObject();
        d2=(IpAddress)ois.readObject();
        e2=(IpAddress)ois.readObject();
        f2=(IpAddress)ois.readObject();
        g2=(IpAddress)ois.readObject();
        h2=(IpAddress)ois.readObject();

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



    public void testStreamable() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      oos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       ois;
        IpAddress             a2, b2, x, x2, y, y2;

        x=new IpAddress(5555);
        x.setAdditionalData(new byte[]{'b','e','l','a'});

        y=new IpAddress(1111);
        y.setAdditionalData(new byte[]{'b','e','l','a'});

        a.setAdditionalData(null);
        b.setAdditionalData("Bela Ban".getBytes());
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

        assert a2.getAdditionalData() == null;
        Assert.assertEquals("Bela Ban", new String(b2.getAdditionalData()));

        assert x2.getAdditionalData() != null;
        Assert.assertEquals(4, x2.getAdditionalData().length);

        assert y2.getIpAddress() != null;
        Assert.assertEquals(1111, y2.getPort());
        assert y2.getAdditionalData() != null;
        Assert.assertEquals(4, y2.getAdditionalData().length);
    }




    public static void testStreamableWithHighPort() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      oos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       dis;
        IpAddress             x, x2;

        x=new IpAddress(65535);
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




}
