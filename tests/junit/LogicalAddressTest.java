// $Id

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.stack.LogicalAddress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class LogicalAddressTest extends TestCase {
    LogicalAddress a, b, c;


    public LogicalAddressTest(String name) {
        super(name);
    }


    public void setUp() throws CloneNotSupportedException {
        a=new LogicalAddress("host1", null);
        b=new LogicalAddress("host1", null);
        c=(LogicalAddress)a.clone();
    }

    public void tearDown() {

    }


    public void testEquality() throws Exception {
        assertFalse(a.equals(b));
        assertFalse(c.equals(b));
        assertTrue(a.equals(c));
        assertTrue(c.equals(a));
    }


    public void testMcast() {
        assertFalse(a.isMulticastAddress());
    }


    public void testCompareTo() {
        assertTrue(a.compareTo(c) == 0);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
    }


    public void testExternalization() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        ObjectOutputStream oos=new ObjectOutputStream(bos);
        byte[] buf=null;
        ByteArrayInputStream bis=null;
        ObjectInputStream ois;
        LogicalAddress a2, b2;

        a.setAdditionalData(null);
        b.setAdditionalData("Bela Ban".getBytes());
        oos.writeObject(a);
        oos.writeObject(b);


        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new ObjectInputStream(bis);
        a2=(LogicalAddress)ois.readObject();
        b2=(LogicalAddress)ois.readObject();

        assertTrue(a.equals(a2));
        assertTrue(b.equals(b2));

        assertTrue(a2.getAdditionalData() == null);
        assertTrue(new String(b2.getAdditionalData()).equals("Bela Ban"));
    }


    public void testExternalizationAdditionalData() throws Exception {
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        ObjectOutputStream oos=new ObjectOutputStream(bos);
        byte[] buf=null;
        ByteArrayInputStream bis=null;
        ObjectInputStream ois;
        LogicalAddress a2, b2, c2;

        oos.writeObject(a);
        oos.writeObject(b);
        oos.writeObject(c);

        buf=bos.toByteArray();
        bis=new ByteArrayInputStream(buf);
        ois=new ObjectInputStream(bis);
        a2=(LogicalAddress)ois.readObject();
        b2=(LogicalAddress)ois.readObject();
        c2=(LogicalAddress)ois.readObject();

        assertTrue(a.equals(a2));
        assertTrue(b.equals(b2));
        assertTrue(c.equals(c2));
        assertTrue(c2.equals(a2));
    }


    public static Test suite() {
        TestSuite s=new TestSuite(LogicalAddressTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
