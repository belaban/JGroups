package org.jgroups.demos;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.jmx.Registration;
import org.jgroups.util.Util;

import javax.management.MBeanServer;

/**
 * Shows how annotations can be used to expose attributes and operations
 * @author Bela Ban
 * @version $Id: JmxDemo.java,v 1.2 2008/03/06 08:51:02 belaban Exp $
 */
@MBean
public class JmxDemo {
    @ManagedAttribute
    private int age;

    @ManagedAttribute
    private static final String last_name="Ban";
    @ManagedAttribute
    private static final String first_name="Bela";

    @ManagedAttribute(description="social security number")
    private static final long id=322649L;

    @ManagedOperation(description="bla")
    public void foo() {
        System.out.println("foo(" + my_number + "): age=" + age + ", name=" + first_name + " " + last_name);
    }

    @ManagedAttribute(writable=true)
    private int my_number=10;

    @ManagedAttribute
    public int getMyNumber() {return my_number;}

    @ManagedAttribute(writable=true)
    public int setMy_number(int num) {my_number=num; return num;}

    @ManagedAttribute
    public void foobar() {} // doesn't start with setXXX() or getXXX(), ignored

    @ManagedOperation
    public String sayName() {
        return "I'm " + first_name + " " + last_name;
    }

    public int add(int a, int b) {return a+b;} // not exposed although @MBean is on the JmxDemo class


    public static void main(String[] args) {
        JmxDemo demo=new JmxDemo();

        MBeanServer server=Util.getMBeanServer();
        if(server != null) {
            try {
                Registration.register(demo, server, "demo:name=DemoObject");
                while(true) {
                    Util.sleep(10000);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
