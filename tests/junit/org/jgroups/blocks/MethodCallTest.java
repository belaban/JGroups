package org.jgroups.blocks;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Id: MethodCallTest.java,v 1.2 2005/02/19 04:25:53 ovidiuf Exp $
 */
public class MethodCallTest extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    //
    // OLD
    //

    public void testOLD() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"});
        assertEquals(MethodCall.OLD, methodCall.getMode());

        Target target = new Target();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    public void testInheritanceOLD() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod", new Object[] {"abc"});
        assertEquals(MethodCall.OLD, methodCall.getMode());

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    //
    // METHOD
    //

    public void testMETHOD() throws Throwable {

        Method method = Target.class.getMethod("someMethod", new Class[] { String.class });
        MethodCall methodCall = new MethodCall(method, new Object[] {"abc"});
        assertEquals(MethodCall.METHOD, methodCall.getMode());

        Target target = new Target();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    public void testInheritanceMETHOD() throws Throwable {

        Method method = Target.class.getMethod("someMethod", new Class[] { String.class });
        MethodCall methodCall = new MethodCall(method, new Object[] {"abc"});
        assertEquals(MethodCall.METHOD, methodCall.getMode());

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    //
    // TYPES
    //

    public void testTYPES() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new Class[] { String.class });
        assertEquals(MethodCall.TYPES, methodCall.getMode());

        Target target = new Target();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    public void testInheritanceTYPES() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new Class[] { String.class });
        assertEquals(MethodCall.TYPES, methodCall.getMode());

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    /**
     * This tests whether overriden methods are correctly identified and invoked.
     */
    public void testOverriddenForTYPES() throws Throwable  {

        MethodCall methodCall = new MethodCall("overriddenMethod",
                                               new Object[] { "abc" },
                                               new Class[] { String.class });
        assertEquals(MethodCall.TYPES, methodCall.getMode());

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        assertEquals("TargetSubclassABC", result);

    }

    public void testNoArgumentMethodForTYPES() throws Throwable  {

        MethodCall methodCall = new MethodCall("noArgumentMethod", new Object[0], new Class[0]);
        assertEquals(MethodCall.TYPES, methodCall.getMode());

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        assertEquals("noArgumentMethodResult", result);

    }


    //
    // SIGNATURE
    //

    public void testSIGNATURE() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new String[] { "java.lang.String" });
        assertEquals(MethodCall.SIGNATURE, methodCall.getMode());

        Target target = new Target();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }

    public void testInheritanceSIGNATURE() throws Throwable {

        MethodCall methodCall = new MethodCall("someMethod",
                                               new Object[] { "abc" },
                                               new String[] { "java.lang.String" });
        assertEquals(MethodCall.SIGNATURE, methodCall.getMode());

        TargetSubclass target = new TargetSubclass();
        Object result = methodCall.invoke(target);
        assertEquals("ABC", result);
    }



    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(MethodCallTest.class));
    }



    class Target {

        public String someMethod(String arg) {
            return arg.toUpperCase();
        }

        public String overriddenMethod(String arg) {
            return "Target" + arg.toUpperCase();
        }

        public String noArgumentMethod() {
            return "noArgumentMethodResult";
        }
    }

    class TargetSubclass extends Target {

        public String overriddenMethod(String arg) {
            return "TargetSubclass" + arg.toUpperCase();
        }

    }

}
