// $Id: MethodCallTest.java,v 1.1 2004/05/15 00:36:47 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.blocks.MethodCall;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Method;



public class MethodCallTest extends TestCase {
    Class cl=MethodCallTest.class;

    public MethodCallTest(String name) {
        super(name);
    }


    public void setUp() {

    }

    public void tearDown() {
        
    }




    public void testMethod() {
        Method m;
        try {
            m=cl.getMethod("foo", new Class[]{int.class, String.class});
            MethodCall mc=new MethodCall(m, new Object[]{new Integer(22), "Bela"});
            assertEquals(mc.invoke(this), Boolean.TRUE);
        }
        catch(Throwable t) {
            fail(t.toString());
        }
    }

    public void testTypes() {
        MethodCall mc;
        mc=new MethodCall("foo", new Object[]{new Integer(35), "Bela"}, new Class[]{int.class, String.class});
        try {
            assertEquals(mc.invoke(this), Boolean.TRUE);
        }
        catch(Throwable t) {
            fail(t.toString());
        }
    }


    public void testSignature() {
        MethodCall mc;
        mc=new MethodCall("foo", new Object[]{new Integer(35), "Bela"},
                          new String[]{int.class.getName(), String.class.getName()});
        try {
            assertEquals(mc.invoke(this), Boolean.TRUE);
        }
        catch(Throwable t) {
            fail(t.toString());
        }
    }

    public boolean foo(int a, String b) {
        System.out.println("test(" + a + ", " + b + ")");
        return true;
    }



    public void testBufferSize() throws Exception {
        int a=10;
        String b="Bela";
        MethodCall m=new MethodCall("foo", new Object[]{new Integer(a),b}, new Class[]{int.class, String.class});
        ByteArrayOutputStream msg_data=new ByteArrayOutputStream();
        ObjectOutputStream msg_out=new ObjectOutputStream(msg_data);
        m.writeExternal(msg_out);
        msg_out.flush();
        msg_out.close();
        byte[] data=msg_data.toByteArray();
        ByteArrayInputStream msg_in_data=new ByteArrayInputStream(data);
        ObjectInputStream msg_in=new ObjectInputStream(msg_in_data);
        MethodCall m2=new MethodCall();
        m2.readExternal(msg_in);
        System.out.println(m2.getName());
        System.out.println(m2.getArgs().length);
    }


    public static Test suite() {
        TestSuite s=new TestSuite(MethodCallTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
