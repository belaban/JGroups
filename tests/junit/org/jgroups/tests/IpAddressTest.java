// $Id: IpAddressTest.java,v 1.4 2004/07/05 14:15:04 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class IpAddressTest extends TestCase {
    IpAddress a, b, c, d, e, f, g, h, i, j, k;

    
    
    public IpAddressTest(String name) {
        super(name);
    }


    public void setUp() {
        a=new IpAddress("localhost", 5555);
        b=new IpAddress("localhost", 5555);
        c=b;
        d=new IpAddress("localhost", 5556);
        e=new IpAddress("127.0.0.1", 5555);
        f=new IpAddress("www.ibm.com", 80);
        g=new IpAddress("www.ibm.com", 8080);
        h=new IpAddress("224.0.0.1", 5555);
    }

    public void tearDown() {
        
    }

 
    public void testEquality() throws Exception {        
        assertTrue(a.equals(b));
        assertTrue(c.equals(b));
        assertTrue(a.equals(e));
        assertTrue(c.equals(e));
    }
   

    public void testInequality() throws Exception {        
        assertTrue(!a.equals(d));
        assertTrue(!c.equals(d));
        assertTrue(!a.equals(f));
        assertTrue(!e.equals(f));
        assertTrue(!f.equals(g));
        assertTrue(!a.equals(null));
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
        IpAddress             a2, b2, c2, d2, e2, f2, g2, h2;
        
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

    
   

    public static Test suite() {
        TestSuite s=new TestSuite(IpAddressTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
