package org.jgroups.blocks;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Id: MethodCallTest.java,v 1.1 2005/02/19 03:33:39 ovidiuf Exp $
 */
public class MethodCallTest extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

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

        public String someMethod(String arg)
        {
            return arg.toUpperCase();
        }
    }

    class TargetSubclass extends Target {
    }

}
