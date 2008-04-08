
package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.Util;
import org.jgroups.Global;
import org.testng.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;


/**
 * @author Bela Ban belaban@yahoo.com
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Id: MethodCallTest.java,v 1.5 2008/04/08 13:21:30 belaban Exp $
 **/
@Test(groups=Global.FUNCTIONAL)
public class MethodCallTest {

    public static class TargetClass {
        public static boolean foo(int a, String b) {
            System.out.println("test(" + a + ", " + b + ')');
            return true;
        }

        public static void bar(String[] a, String b) {
            if(a != null) {
                for(int i=0; i < a.length; i++) {
                    String s=a[i];
                    System.out.print(s + ' ');
                }
            }
            else
                System.out.println("a=null");
            if(b != null)
                System.out.println("b=" + b);
            else
                System.out.println("b=null");
        }

        public static void foobar() {
            System.out.println("foobar()");
        }
    }


    final TargetClass target=new TargetClass();


    public void testOld() {
        try {
            MethodCall mc=new MethodCall("foo", new Object[]{new Integer(22), "Bela"}, new Class[]{int.class,String.class});
            Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testOld2() {
        try {
            MethodCall mc=new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, "Bela"},
                                         new Class[]{String[].class, String.class});
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testWithNull() {
        try {
            MethodCall mc=new MethodCall("foobar", null, (Class[])null);
            System.out.println("mc: " + mc);
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testOldWithNull() {
        try {
            MethodCall mc=new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, null},
                                         new Class[]{String[].class, String.class});
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testOldWithNull2() {
        try {
            MethodCall mc=new MethodCall("bar", new Object[]{null, "Bela"},
                                         new Class[]{String[].class, String.class});
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testOldWithNull3() {
        try {
            MethodCall mc=new MethodCall("foobar", null, (Class[])null);
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testOldWithNull4() {
        try {
            MethodCall mc=new MethodCall("foobar", new Object[0], (Class[])null);
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testMethod() {
        Method m;
        try {
            m=TargetClass.class.getMethod("foo", new Class[]{int.class, String.class});
            MethodCall mc=new MethodCall(m, new Object[]{new Integer(22), "Bela"});
            Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testTypes() {
        MethodCall mc;
        mc=new MethodCall("foo", new Object[]{new Integer(35), "Bela"}, new Class[]{int.class, String.class});
        try {
            Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }



    public void testTypesWithArray() {
        MethodCall mc;
        mc=new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, "Bela"},
                          new Class[]{String[].class, String.class});
        try {
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testTypesWithNullArgument() {
        MethodCall mc;
        mc=new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, null},
                          new Class[]{String[].class, String.class});
        try {
            mc.invoke(target);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testTypesWithNullArgument2() throws Throwable {
        MethodCall mc;
        mc=new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, new Object[]{}},
                          new Class[]{String[].class, String.class});
        mc.invoke(target);
    }


    public void testTypesWithNullArgument3() {
        MethodCall mc;
        mc=new MethodCall("foobar", new Object[]{}, new Class[]{});
        try {
            mc.invoke(target);
        }
        catch(IllegalArgumentException ex) {
            assert true : "this was expected";
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testTypesWithNullArgument4() {
        MethodCall mc;
        mc=new MethodCall("foobar", null, (Class[])null);
        try {
            mc.invoke(target);
        }
        catch(IllegalArgumentException ex) {
            assert true : "this was expected";
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }


    public void testTypesWithNullArgument5() {
        MethodCall mc;
        mc=new MethodCall("foobar", new Object[0], new Class[0]);
        try {
            mc.invoke(target);
        }
        catch(IllegalArgumentException ex) {
            assert true : "this was expected";
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }



    public void testSignature() {
        MethodCall mc;
        mc=new MethodCall("foo", new Object[]{new Integer(35), "Bela"},
                          new String[]{int.class.getName(), String.class.getName()});
        try {
            Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
        }
        catch(Throwable t) {
            assert false : t.toString();
        }
    }



    public static void testBufferSize() throws Exception {
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


    public static void testOLD() throws Throwable {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[]{String.class});
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceOLD() throws Throwable {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[]{String.class});
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testMETHOD() throws Throwable {

        Method method = Target.class.getMethod("someMethod", new Class[] { String.class });
        MethodCall methodCall = new MethodCall(method, new Object[] {"abc"});

        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceMETHOD() throws Throwable {

        Method method = Target.class.getMethod("someMethod", new Class[] { String.class });
        MethodCall methodCall = new MethodCall(method, new Object[] {"abc"});

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testTYPES() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new Class[] { String.class });

        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceTYPES() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new Class[] { String.class });

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }

    /**
     * This tests whether overriden methods are correctly identified and invoked.
     */
    public static void testOverriddenForTYPES() throws Throwable  {

        MethodCall methodCall = new MethodCall("overriddenMethod",
                                               new Object[] { "abc" },
                                               new Class[] { String.class });

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("TargetSubclassABC", result);

    }


    public static void testNoArgumentMethodForTYPES() throws Throwable  {

        MethodCall methodCall = new MethodCall("noArgumentMethod", new Object[0], new Class[0]);

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("noArgumentMethodResult", result);

    }



    public static void testSIGNATURE() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new String[] { "java.lang.String" });

        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceSIGNATURE() throws Throwable {
        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new String[] { "java.lang.String" });

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }



    public static void testMarshalling() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new String[] { "java.lang.String" });
        methodCall.put("name", "Bela");
        methodCall.put("id", new Integer(322649));

        System.out.println("methodCall: " + methodCall);

        MethodCall m=marshalAndUnmarshal(methodCall);
        System.out.println("m: " + m);
        Assert.assertEquals(m.get("name"), "Bela");
        Assert.assertEquals(m.get("id"), new Integer(322649));
    }


    private static MethodCall marshalAndUnmarshal(MethodCall m) throws Exception {
        byte[] buf=Util.objectToByteBuffer(m);
        return (MethodCall)Util.objectFromByteBuffer(buf);
    }


    public static class Target {

        public static String someMethod(String arg) {
            return arg.toUpperCase();
        }

        public String overriddenMethod(String arg) {
            return "Target" + arg.toUpperCase();
        }

        public static String noArgumentMethod() {
            return "noArgumentMethodResult";
        }
    }

    public static class TargetSubclass extends Target {

        public String overriddenMethod(String arg) {
            return "TargetSubclass" + arg.toUpperCase();
        }

    }


}
