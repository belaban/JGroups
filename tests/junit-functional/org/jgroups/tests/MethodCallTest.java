
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


/**
 * @author Bela Ban belaban@yahoo.com
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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


    public void testOld() throws Exception {
        MethodCall mc=new MethodCall("foo", new Object[]{new Integer(22), "Bela"}, new Class[]{int.class,String.class});
        Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
    }


    public void testOld2() throws Exception {
        new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, "Bela"},
                       new Class[]{String[].class, String.class}).invoke(target);
    }


    public void testWithNull() throws Exception {
        new MethodCall("foobar", null, null).invoke(target);
    }


    public void testOldWithNull() throws Exception {
        new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, null},
                       new Class[]{String[].class, String.class}).invoke(target);
    }


    public void testOldWithNull2() throws Exception {
        new MethodCall("bar", new Object[]{null, "Bela"},
                       new Class[]{String[].class, String.class}).invoke(target);
    }


    public void testOldWithNull3() throws Exception {
        new MethodCall("foobar", null, null).invoke(target);
    }


    public void testOldWithNull4() throws Exception {
        new MethodCall("foobar", new Object[0], null).invoke(target);
    }


    public void testMethod() throws Exception {
        Method m=TargetClass.class.getMethod("foo", new Class[]{int.class, String.class});
        MethodCall mc=new MethodCall(m, new Integer(22), "Bela");
        Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
    }


    public void testTypes() throws Exception {
        MethodCall mc=new MethodCall("foo", new Object[]{new Integer(35),"Bela"}, new Class[]{int.class,String.class});
        Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
    }


    public void testTypesWithArray() throws Exception {
        new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, "Bela"},
                       new Class[]{String[].class, String.class}).invoke(target);
    }


    public void testTypesWithNullArgument() throws Exception {
        new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, null},
                       new Class[]{String[].class, String.class}).invoke(target);
    }

    public void testTypesWithNullArgument2() throws Exception {
        MethodCall mc=new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, new Object[]{}},
                                     new Class[]{String[].class, String.class});
        try {
            mc.invoke(target);
            assert false: "we should not get here as there should be an argument mismatch exception";
        }
        catch(IllegalArgumentException ex) {
            System.out.println("caught IllegalArgumentException - as expected");
        }
    }


    public void testTypesWithNullArgument3() throws Exception {
        new MethodCall("foobar", new Object[]{}, new Class[]{}).invoke(target);
    }


    public void testTypesWithNullArgument4() throws Exception {
        new MethodCall("foobar", null, null).invoke(target);
    }


    public void testTypesWithNullArgument5() throws Exception {
        new MethodCall("foobar", new Object[0], new Class[0]).invoke(target);
    }


    public void testSignature() throws Exception {
        MethodCall mc=new MethodCall("foo", new Object[]{new Integer(35), "Bela"},
                                     new Class[]{int.class, String.class});
        Assert.assertEquals(mc.invoke(target), Boolean.TRUE);
    }



    public static void testBufferSize() throws Exception {
        MethodCall m=new MethodCall("foo", new Object[]{new Integer(10),"Bela"}, new Class[]{int.class, String.class});
        byte[] data=Util.objectToByteBuffer(m);
        
        MethodCall m2=(MethodCall)Util.objectFromByteBuffer(data);
        System.out.println(m2);
        Object[] args=m2.getArgs();
        assert args.length == 2;
        assert args[0].equals(new Integer(10));
        assert args[1].equals("Bela");
    }


    public static void testOLD() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[]{String.class});
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceOLD() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[]{String.class});
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testMETHOD() throws Exception {
        Method method = Target.class.getMethod("someMethod", new Class[] { String.class });
        MethodCall methodCall = new MethodCall(method, "abc");
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceMETHOD() throws Exception {
        Method method = Target.class.getMethod("someMethod", new Class[] { String.class });
        MethodCall methodCall = new MethodCall(method, "abc");
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testTYPES() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] { String.class });
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceTYPES() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] { String.class });
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }

    /**
     * This tests whether overriden methods are correctly identified and invoked.
     */
    public static void testOverriddenForTYPES() throws Exception  {
        MethodCall methodCall = new MethodCall("overriddenMethod", new Object[] { "abc" }, new Class[] { String.class });
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("TargetSubclassABC", result);
    }


    public static void testNoArgumentMethodForTYPES() throws Exception  {
        MethodCall methodCall = new MethodCall("noArgumentMethod", new Object[0], new Class[0]);
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("noArgumentMethodResult", result);
    }



    public static void testSIGNATURE() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] {String.class});
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }


    public static void testInheritanceSIGNATURE() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[] {String.class});
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC", result);
    }



    public static void testMarshalling() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] {String.class});
        System.out.println("methodCall: " + methodCall);
        MethodCall m=marshalAndUnmarshal(methodCall);
        System.out.println("m: " + m);
    }

    public static void testMarshalling2() throws Exception {
        Address addr=Util.createRandomAddress("A");
        MethodCall call=new MethodCall("foo",
                                       new Object[]{"hello", 23, addr},
                                       new Class[]{String.class, int.class, Address.class});
        MethodCall m=marshalAndUnmarshal(call);
        System.out.println("m = " + m);
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
