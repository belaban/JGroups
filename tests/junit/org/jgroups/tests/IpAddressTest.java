// $Id: IpAddressTest.java,v 1.11 2005/04/19 08:34:44 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.HashMap;


public class IpAddressTest extends TestCase {
    IpAddress a, b, c, d, e, f, g, h, i, j, k;

    
    
    public IpAddressTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        a=new IpAddress("localhost", 5555);
        b=new IpAddress("localhost", 5555);
        c=b;
        d=new IpAddress("localhost", 5556);
        e=new IpAddress("127.0.0.1", 5555);
        f=new IpAddress("www.ibm.com", 80);
        g=new IpAddress("www.ibm.com", 8080);
        h=new IpAddress("224.0.0.1", 5555);
    }



    public void testUnknownAddress() throws UnknownHostException {
        IpAddress addr=new IpAddress("idontknow.com", 55);
        assertEquals(addr.getIpAddress(), InetAddress.getLocalHost());
    }

    public void testEquality() throws Exception {
        assertTrue(a.equals(b));
        assertTrue(c.equals(b));
        assertTrue(a.equals(e));
        assertTrue(c.equals(e));
    }

    public void testEqualityWithDnsRoundRobin() throws UnknownHostException {
        IpAddress x1, x2, x3;

        InetAddress addr=InetAddress.getByName("127.0.0.1");
        byte[] rawAddr=addr.getAddress();

        InetAddress inet1=InetAddress.getByAddress("MyHost1", rawAddr);
        InetAddress inet2=InetAddress.getByAddress("MyHost2", rawAddr);
        InetAddress inet3=InetAddress.getByAddress("MyHost3", rawAddr);
        assertEquals(inet1, inet2);

        x1=new IpAddress(inet1, 5555);
        x2=new IpAddress(inet2, 5555);
        x3=new IpAddress(inet3, 5555);

        assertEquals(x1, x2);
        assertEquals(x3, x1);

        HashSet s=new HashSet();
        s.add(x1);
        s.add(x2);
        s.add(x3);
        System.out.println("s=" + s);
        assertEquals(1, s.size());

        HashMap m=new HashMap();
        m.put(x1, "Bela");
        m.put(x2, "Michelle");
        m.put(x3, "Nicole");
        assertEquals(1, m.size());
        assertEquals("Nicole", m.get(x1));
    }


    public void testInequality() throws Exception {
        assertTrue(!a.equals(d));
        assertTrue(!c.equals(d));
        assertTrue(!a.equals(f));
        assertTrue(!e.equals(f));
        assertTrue(!f.equals(g));
        assertFalse(a.equals(null));
    }


    public void testSameHost() throws Exception {        
        assertTrue(Util.sameHost(a, b));
        assertTrue(Util.sameHost(a, c));
        assertTrue(Util.sameHost(a, d));
        assertTrue(Util.sameHost(a, e));
        assertTrue(Util.sameHost(f, g));
    }


    public void testNotSameHost() throws Exception {        
        assertTrue(!Util.sameHost(a, f));
        assertTrue(!Util.sameHost(e, f));
        assertTrue(!Util.sameHost(e, null));
        assertTrue(!Util.sameHost(null, null));
    }

    public void testMcast() {
        assertTrue(h.isMulticastAddress());
        assertTrue(!a.isMulticastAddress());
        assertTrue(!e.isMulticastAddress());
        assertTrue(!g.isMulticastAddress());

    }

    
    public void testCompareTo() {
        assertTrue(a.compareTo(b) == 0);
        assertTrue(a.compareTo(d) < 0);
        assertTrue(d.compareTo(a) > 0);
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

        assertTrue(a.equals(a2));
        assertTrue(b.equals(b2));
        
        assertTrue(a2.getAdditionalData() == null);
        assertTrue("Bela Ban".equals(new String(b2.getAdditionalData())));
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

         assertTrue(b2.equals(c2));
         assertTrue(a.equals(a2));
         assertTrue(b.equals(b2));
         assertTrue(c.equals(c2));
         assertTrue(d.equals(d2));
         assertTrue(e.equals(e2));
         assertTrue(f.equals(f2));
         assertTrue(g.equals(g2));
         assertTrue(h.equals(h2));
     }


    public void testStreamable() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        DataOutputStream      oos=new DataOutputStream(bos);
        byte[]                buf=null;
        ByteArrayInputStream  bis=null;
        DataInputStream       ois;
        IpAddress             a2, b2, x, x2;

        x=new IpAddress(5555);
        x.setAdditionalData(new byte[]{'b','e','l','a'});

        a.setAdditionalData(null);
        b.setAdditionalData("Bela Ban".getBytes());
        a.writeTo(oos);
        b.writeTo(oos);
        x.writeTo(oos);

        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new DataInputStream(bis);
        a2=new IpAddress();
        a2.readFrom(ois);
        b2=new IpAddress();
        b2.readFrom(ois);
        x2=new IpAddress();
        x2.readFrom(ois);

        assertTrue(a.equals(a2));
        assertTrue(b.equals(b2));

        assertNull(a2.getAdditionalData());
        assertEquals("Bela Ban", new String(b2.getAdditionalData()));

        assertNotNull(x2.getAdditionalData());
        assertEquals(x2.getAdditionalData().length, 4);
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

        assertTrue(b2.equals(c2));
        assertTrue(a.equals(a2));
        assertTrue(b.equals(b2));
        assertTrue(c.equals(c2));
        assertTrue(d.equals(d2));
        assertTrue(e.equals(e2));
        assertTrue(f.equals(f2));
        assertTrue(g.equals(g2));
        assertTrue(h.equals(h2));
    }



    public static Test suite() {
        TestSuite s=new TestSuite(IpAddressTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
