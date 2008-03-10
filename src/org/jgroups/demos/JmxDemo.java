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
 * @version $Id: JmxDemo.java,v 1.7 2008/03/10 08:57:57 vlada Exp $
 */
@MBean
public class JmxDemo {
    @ManagedAttribute
    private int age;   // exposed as read-only 'age'

    @ManagedAttribute
    private static final String last_name="Ban";  // read-only (because final) 'last_name'

    @ManagedAttribute
    private static final String first_name="Bela";  // read-only (final) 'first_name'

    @ManagedAttribute(description="social security number") // read-only
    private static final long id=322649L;

    public void foo() { // must be exposed because we have @MBean on the class
        System.out.println("foo(" + number + "): age=" + age + ", name=" + first_name + " " + last_name);
    }

    @ManagedAttribute
    private int number=10; // writeable because we have the (non-annnotated) setter below !!

    public void setNumber(int num) {number=num;}

    @ManagedAttribute
    public int getMyFoo() {return 22;} // exposed as 'MyFoo' *not* 'getMyFoo()' !! 

    @ManagedAttribute
    private int other_number=20;  // exposed as 'otherNumber' ?
    public int getOtherNumber() {return other_number;}   // this should show up as 'otherNumber'
    public void setOtherNumber(int num) {other_number=num;}

    @ManagedAttribute
    public void foobar() {} // doesn't start with setXXX() or getXXX(), ignored

    @ManagedAttribute
    public boolean isFlag() {return true;} // exposed as Flag, *not* 'isFlag()' !!

    @ManagedAttribute(description="my number attribute")
    private long my_number=322649L;

    public void setMyNumber(long new_number) {
        my_number=new_number;
    }

    private int accountNumber=10;

    @ManagedAttribute
    public void setAccountNumber(int num) {accountNumber=num;} // exposes accountNumber as writable

    @ManagedAttribute
    public int getAccountNumber() {return accountNumber;}

    int max_age=100;

    @ManagedAttribute
    public void setMaxAge(int age) {max_age=age;}

    @ManagedAttribute
    public int getMaxAge() {return max_age;}
    


    @ManagedOperation
    public String sayName() {
        return "I'm " + first_name + " " + last_name;
    }

    public int add(int a, int b) {return a+b;} // exposed because @MBean is on the class


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
