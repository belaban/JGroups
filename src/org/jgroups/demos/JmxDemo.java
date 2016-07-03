package org.jgroups.demos;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

/**
 * Shows how annotations can be used to expose attributes and operations
 * @author Bela Ban
 */
@MBean
public class JmxDemo extends NotificationBroadcasterSupport {
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

    @ManagedAttribute
    public void setNumber(int num) {number=num;}

    @ManagedAttribute
    public String getNumberAsString() {
        return String.valueOf(number);
    }

    @ManagedAttribute
    public static int getMyFoo() {return 22;} // exposed as 'myFoo' *not* 'getMyFoo()' !!

    @ManagedAttribute(writable=true)
    private static int my_other_number_is_here=999; // exposed as writable myOtherNumberIsHere

    @ManagedAttribute
    private int other_number=20;  // exposed as 'otherNumber' ?

    @ManagedAttribute
    public void setOtherNumber(int num) {other_number=num;}

    @ManagedAttribute
    public void foobar() {} // doesn't start with setXXX() or getXXX(), ignored

    @ManagedAttribute
    public static boolean isFlag() {return true;} // exposed as Flag, *not* 'isFlag()' !!

    @ManagedAttribute(description="my number attribute")
    private long my_number=322649L;


    @ManagedAttribute
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
        demo.addNotificationListener((notification, handback) -> System.out.println(">> " + notification + ", handback=" + handback), null, "myHandback");
        demo.startNotifications();

        MBeanServer server=Util.getMBeanServer();
        if(server != null) {
            try {
                JmxConfigurator.register(demo, server, "demo:name=DemoObject");
                while(true) {
                    Util.sleep(10000);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void startNotifications() {
        new Thread() {
            @Override
            public void run() {
                int cnt=1;
                while(true) {
                    Util.sleep(1000);
                    MyNotification notif=new MyNotification("home.grown", this, cnt, "hello-" + cnt);
                    notif.setName("Bela Ban");
                    cnt++;
                    sendNotification(notif);
                }
            }
        }.start();
    }

    protected static class MyNotification extends Notification {
        protected String name;

        public MyNotification(String type, Object source, long sequenceNumber) {
            super(type, source, sequenceNumber);
        }

        public MyNotification(String type, Object source, long sequenceNumber, String message) {
            super(type, source, sequenceNumber, message);
        }

        public MyNotification(String type, Object source, long sequenceNumber, long timeStamp) {
            super(type, source, sequenceNumber, timeStamp);
        }

        public MyNotification(String type, Object source, long sequenceNumber, long timeStamp, String message) {
            super(type, source, sequenceNumber, timeStamp, message);
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return super.toString() + ", name=" + name;
        }
    }
}
