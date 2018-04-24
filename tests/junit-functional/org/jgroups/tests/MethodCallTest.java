
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Bela Ban belaban@yahoo.com
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 **/
@Test(groups=Global.FUNCTIONAL)
public class MethodCallTest {

    protected static final Method CALL;
    protected static final Map<Short,Method> methods=new HashMap<>();

    static {
        try {
            CALL=MethodCallTest.class.getMethod("call", boolean.class, Boolean.class, int.class, double.class,
                                                float.class, byte[].class, String[].class);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        methods.put((short)1, CALL);

        try {
            methods.put((short)2, TargetClass.class.getMethod("foobar"));
        }
        catch(NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }


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

    @Test(enabled=false)
    public static void call(boolean b, Boolean bool, int i, double d, float f, byte[] buf, String[] strings) {
        System.out.println("b = " + b + ", bool=" + bool + ", i=" + i + ", d=" + d + ", f=" + f +
                             ", buf=" + buf.length + " bytes, strings=" + Arrays.toString(strings));
    }

    final TargetClass target=new TargetClass();



    public void testMethod() throws Exception {
        Method m=TargetClass.class.getMethod("foo", int.class, String.class);
        MethodCall mc=new MethodCall(m,22, "Bela");
        Assert.assertEquals(mc.invoke(target),Boolean.TRUE);
    }


    public void testMethod2() throws Exception {
        MethodCall call=new MethodCall(CALL, true, Boolean.FALSE, 322649, 3.24, (float)54.345,
                                       new byte[]{'b', 'e', 'l', 'a'}, new String[]{"Bela", "Michelle"});
        call.invoke(this);
    }


    public void testTypes() throws Exception {
        MethodCall mc=new MethodCall("foo", new Object[]{35,"Bela"}, new Class[]{int.class,String.class});
        Assert.assertEquals(mc.invoke(target),Boolean.TRUE);
    }

    public void testTypes2() throws Exception {
        new MethodCall("bar", new Object[]{new String[]{"one", "two", "three"}, "Bela"},
                       new Class[]{String[].class, String.class}).invoke(target);
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
        MethodCall mc=new MethodCall("foo", new Object[]{35, "Bela"},
                                     new Class[]{int.class, String.class});
        Assert.assertEquals(mc.invoke(target),Boolean.TRUE);
    }


    public static void testBufferSize() throws Exception {
        MethodCall m=new MethodCall("foo", new Object[]{10,"Bela"}, new Class[]{int.class, String.class});
        byte[] data=Util.objectToByteBuffer(m);
        
        MethodCall m2=Util.objectFromByteBuffer(data);
        System.out.println(m2);
        Object[] args=m2.args();
        assert args.length == 2;
        assert args[0].equals(10);
        assert args[1].equals("Bela");
    }


    public static void testOLD() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[]{String.class});
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
    }


    public static void testInheritanceOLD() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[]{String.class});
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
    }


    public static void testMETHOD() throws Exception {
        Method method = Target.class.getMethod("someMethod", String.class);
        MethodCall methodCall = new MethodCall(method, "abc");
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
    }


    public static void testInheritanceMETHOD() throws Exception {
        Method method = Target.class.getMethod("someMethod", String.class);
        MethodCall methodCall = new MethodCall(method, "abc");
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
    }


    public static void testTYPES() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] { String.class });
        Target target = new Target();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
    }


    public static void testInheritanceTYPES() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] { String.class });
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
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
        Assert.assertEquals("ABC",result);
    }


    public static void testInheritanceSIGNATURE() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"}, new Class[] {String.class});
        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        Assert.assertEquals("ABC",result);
    }



    public static void testMarshalling() throws Exception {
        MethodCall methodCall = new MethodCall("someMethod", new Object[] { "abc" }, new Class[] {String.class});
        System.out.println("methodCall: " + methodCall);
        MethodCall m=marshalAndUnmarshal(methodCall);
        System.out.println("m: " + m);
    }

    public void testMarshalling2() throws Exception {
        Address addr=Util.createRandomAddress("A");
        MethodCall call=new MethodCall("foo",
                                       new Object[]{"hello", 23, addr},
                                       new Class[]{String.class, int.class, Address.class});
        MethodCall m=marshalAndUnmarshal(call);
        System.out.println("m = " + m);
    }

    public static void testMarshallingMETHOD() throws Exception {
        Method m=TargetClass.class.getMethod("foo", int.class, String.class);
        MethodCall mc=new MethodCall(m,22, "Bela");
        MethodCall call2=marshalAndUnmarshal(mc);
        System.out.println("call2 = " + call2);

        mc=new MethodCall(CALL, true, Boolean.FALSE, 322649, 3.24, (float)54.345,
                          new byte[]{'b', 'e', 'l', 'a'}, new String[]{"Bela", "Michelle"});
        call2=marshalAndUnmarshal(mc);
        System.out.println("call2 = " + call2);
    }

    public static void testMarshallingTYPES() throws Exception {
        MethodCall mc=new MethodCall("call", new Object[]
                                     {true,Boolean.FALSE,322649,3.24,(float)54.345,
                                       new byte[]{'b','e','l','a'},new String[]{"Bela","Michelle"}},
                                     new Class<?>[]{boolean.class, Boolean.class, int.class, double.class, float.class,
                                       byte[].class, String[].class});
        MethodCall call2=marshalAndUnmarshal(mc);
        System.out.println("call2 = " + call2);
    }

    public void testMarshallingID() throws Exception {
        MethodCall mc=new MethodCall((short)1, true, Boolean.FALSE, 322649, 3.24, (float)54.345,
                                     new byte[]{'b', 'e', 'l', 'a'}, new String[]{"Bela", "Michelle"});
        MethodCall call2=marshalAndUnmarshal(mc);
        System.out.println("call2 = " + call2);

        mc=new MethodCall((short)1, true, Boolean.FALSE, 322649, 3.24, (float)54.345,
                          new byte[]{'b', 'e', 'l', 'a'}, new String[]{"Bela", "Michelle"});
        call2=marshalAndUnmarshal(mc);
        System.out.println("call2 = " + call2);


        mc=new MethodCall((short)2);
        call2=marshalAndUnmarshal(mc);
        System.out.println("call2 = " + call2);
    }


    private static MethodCall marshalAndUnmarshal(MethodCall m) throws Exception {
        byte[] buf=Util.objectToByteBuffer(m);
        System.out.println("marshalled buffer size: " + buf.length + " bytes");
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
