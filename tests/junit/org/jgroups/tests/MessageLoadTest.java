package org.jgroups.tests;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.debug.Debugger;

import java.util.Iterator;
import java.util.Vector;

/**
 * This test case checks how high volume of messages are handled by JGroups
 * using <code>UDP</code> or <code>LOOPBACK</code> protocols. It can be run
 * as JUnit test case or in the command line. Parameters are:
 * <ul>
 * <li><code>-sleep n</code> - means that after each message sending, sender
 * thread will sleep for <code>n</code> milliseconds;
 * <li><code>-msg_num n</code> - <code>n</code> is number of messages to send;
 * <li><code>-loopback</code> - switch test case to loopback mode;
 * <li><code>-debug</code> - pop-up protocol debugger;
 * <li><code>-cummulative</code> - debugger shows cummulative messages.
 * </ul>
 * $Id: MessageLoadTest.java,v 1.6 2004/07/05 14:15:04 belaban Exp $
 */
public class MessageLoadTest extends TestCase {

    public static boolean USE_DEBUGGER=false;

    public static boolean CUMMULATIVE=false;

    public static boolean LOOPBACK=false;

    public static int MESSAGE_NUMBER=5 * 1000;

    public static boolean SLEEP_BETWEEN_SENDING=true;

    public static int SLEEP_TIME=1;

    /** Protocol for UDP mode. */
    public static final String NETWORK_TRANSPORT=
            "UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=1;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;down_thread=false):";

    /** Protocol for loopback mode. */
    public static final String LOOPBACK_TRANSPORT=
            "LOOPBACK(down_thread=false;up_thread=false):";

    /** Generic part of protocol. */
    public static final String JAVAGROUPS_STACK=
            "PING(timeout=500;num_initial_members=1;down_thread=false;up_thread=false):" +
            "FD(down_thread=false;up_thread=false):" +
            "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800;down_thread=false):" +
            "pbcast.STABLE(desired_avg_gossip=200;down_thread=false;up_thread=false):" +
            "FRAG(frag_size=4096):" +
            //"UNICAST(timeout=5000):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=1000;" +
            "shun=false;print_local_addr=false;down_thread=true;up_thread=true)";
    //"SPEED_LIMIT(down_queue_limit=10):" +
    //"pbcast.STATE_TRANSFER(down_thread=false)";


    protected Log log=LogFactory.getLog(this.getClass());


    /**
     * This method returns the protocol stack depending on loopback option.
     */
    public static String getProtocolStack() {
        return (LOOPBACK? LOOPBACK_TRANSPORT : NETWORK_TRANSPORT) + JAVAGROUPS_STACK;
    }

    /**
     * Constructor to create test case.
     */
    public MessageLoadTest(String string) {
        super(string);
    }

    protected JChannel channel1;
    protected PullPushAdapter adapter1;
    protected Debugger debugger1;

    protected JChannel channel2;
    protected PullPushAdapter adapter2;
    protected Debugger debugger2;

    /**
     * Print selected options before test starts.
     */
    protected static void printSelectedOptions() {
        System.out.println("will sleep : " + SLEEP_BETWEEN_SENDING);
        if(SLEEP_BETWEEN_SENDING)
            System.out.println("sleep time : " + SLEEP_TIME);

        System.out.println("msg num : " + MESSAGE_NUMBER);

        System.out.println("loopback : " + LOOPBACK);

    }

    /**
     * Set up unit test. This method instantiates one or two channels depending
     * if test case works in loopback mode or not. Also it might add protocol
     * stack debuggers if such option was selected at startup.
     */
    protected void setUp() throws Exception {
        printSelectedOptions();

        channel1=new JChannel(getProtocolStack());
        System.out.print("Connecting to channel...");
        channel1.connect("LOAD_TEST");
        System.out.println("connected.");

        adapter1=new PullPushAdapter(channel1);

        if(USE_DEBUGGER) {
            debugger1=new Debugger(channel1, CUMMULATIVE, "channel 1");
            debugger1.start();
        }

        // sleep one second before second member joins
        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException ex) {
        }

        if(!LOOPBACK) {
            channel2=new JChannel(getProtocolStack());
            channel2.connect("LOAD_TEST");


            adapter2=new PullPushAdapter(channel2);

            if(USE_DEBUGGER) {
                debugger2=new Debugger(channel2, CUMMULATIVE, "channel 2");
                debugger2.start();
            }

            // sleep one second before processing continues
            try {
                Thread.sleep(1000);
            }
            catch(InterruptedException ex) {
            }
        }
    }

    /**
     * Tears down test case. This method closes all opened channels.
     */
    protected void tearDown() throws Exception {
        if(!LOOPBACK) {
            adapter2.stop();
            channel2.close();
        }

        adapter1.stop();
        channel1.close();
    }

    protected boolean finishedReceiving;

    /**
     * Test method. This method adds a message listener to the PullPushAdapter
     * on channel 1, and starts sending specified number of messages into
     * channel 1 or 2 depending if we are in loopback mode or not. Each message
     * containg timestamp when it was created. By measuring time on message
     * delivery we can calculate message trip time. Listener is controlled by
     * two string messages "start" and "stop". After sender has finished to
     * send messages, it waits until listener receives all messages or "stop"
     * message. Then test is finished and calculations are showed.
     * <p/>
     * Also we calculate how much memory
     * was allocated before excuting a test and after executing a test.
     */
    public void testLoad() {
        try {
            final String startMessage="start";
            final String stopMessage="stop";

            final Object mutex=new Object();

            final Vector receivedTimes=new Vector(MESSAGE_NUMBER);
            final Vector normalMessages=new Vector(MESSAGE_NUMBER);
            final Vector tooQuickMessages=new Vector();
            final Vector tooSlowMessages=new Vector();

            if(USE_DEBUGGER) {
                System.out.println("Press any key to continue...");
                try {
                    System.in.read();
                }
                catch(java.io.IOException ioex) {
// do nothing
                }
            }

            adapter1.setListener(new MessageListener() {
                private boolean started=false;
                private boolean stopped=false;

                public byte[] getState() {
                    return null;
                }

                public void setState(byte[] state) {
// do nothing...
                }

                public void receive(Message jgMessage) {
                    Object message=jgMessage.getObject();

                    if(startMessage.equals(message)) {
                        started=true;
                        finishedReceiving=false;
                    }
                    else if(stopMessage.equals(message)) {
                        stopped=true;
                        finishedReceiving=true;

                        synchronized(mutex) {
                            mutex.notifyAll();
                        }

                    }
                    else if(message instanceof Long) {
                        Long travelTime=new Long(
                                System.currentTimeMillis() - ((Long)message).longValue());

                        if(!started)
                            tooQuickMessages.add(message);
                        else if(started && !stopped) {
                            receivedTimes.add(travelTime);
                            normalMessages.add(message);
                        }
                        else
                            tooSlowMessages.add(message);
                    }
                }
            });

            System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
            System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());
            System.out.println("Starting sending messages.");

            long time=System.currentTimeMillis();

            Message startJgMessage=new Message();
            startJgMessage.setObject(startMessage);

            JChannel sender=LOOPBACK? channel1 : channel2;

            sender.send(startJgMessage);

            for(int i=0; i < MESSAGE_NUMBER; i++) {
                Long message=new Long(System.currentTimeMillis());

                Message jgMessage=new Message();
                jgMessage.setObject(message);

                sender.send(jgMessage);

                if(i % 1000 == 0)
                    System.out.println("sent " + i + " messages.");

                if(SLEEP_BETWEEN_SENDING)
//try {Thread.sleep(SLEEP_TIME);} catch (InterruptedException ex) {}
                    org.jgroups.util.Util.sleep(1, true);
//Thread.yield();

            }

            Message stopJgMessage=new Message();
            stopJgMessage.setObject(stopMessage);
            sender.send(stopJgMessage);

            time=System.currentTimeMillis() - time;

            System.out.println("Finished sending messages. Operation took " + time);

            synchronized(mutex) {

                int received=0;

                while(!finishedReceiving) {
                    mutex.wait(1000);

                    if(receivedTimes.size() != received) {
                        received=receivedTimes.size();

                        System.out.println();
                        System.out.print("Received " + receivedTimes.size() + " messages.");
                    }
                    else {
                        System.out.print(".");
                    }
                }
            }

            try {
                Thread.sleep(1000);
            }
            catch(Exception ex) {
            }

            double avgDeliveryTime=-1.0;
            long maxDeliveryTime=Long.MIN_VALUE;
            long minDeliveryTime=Long.MAX_VALUE;

            Iterator iterator=receivedTimes.iterator();
            while(iterator.hasNext()) {
                Long message=(Long)iterator.next();

                if(avgDeliveryTime == -1.0)
                    avgDeliveryTime=message.longValue();
                else
                    avgDeliveryTime=(avgDeliveryTime + message.doubleValue()) / 2.0;

                if(message.longValue() > maxDeliveryTime)
                    maxDeliveryTime=message.longValue();

                if(message.longValue() < minDeliveryTime)
                    minDeliveryTime=message.longValue();
            }

            System.out.println("Sent " + MESSAGE_NUMBER + " messages.");
            System.out.println("Received " + receivedTimes.size() + " messages.");
            System.out.println("Average delivery time " + avgDeliveryTime + " ms");
            System.out.println("Minimum delivery time " + minDeliveryTime + " ms");
            System.out.println("Maximum delivery time " + maxDeliveryTime + " ms");
            System.out.println("Received " + tooQuickMessages.size() + " too quick messages");
            System.out.println("Received " + tooSlowMessages.size() + " too slow messages");
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
        System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());

        System.out.println("Performing GC");

        Runtime.getRuntime().gc();

        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException ex) {
        }

        System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
        System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());

        if(USE_DEBUGGER) {
            System.out.println("Press any key to finish...");
            try {
                System.in.read();
            }
            catch(java.io.IOException ioex) {
            }
        }
    }

    /**
     * Main method to start a test case from the command line. Parameters are:
     * <ul>
     * <li><code>-sleep n</code> - means that after each message sending, sender
     * thread will sleep for <code>n</code> milliseconds;
     * <li><code>-msg_num n</code> - <code>n</code> is number of messages to send;
     * <li><code>-loopback</code> - switch test case to loopback mode;
     * <li><code>-debug</code> - pop-up protocol debugger;
     * <li><code>-cummulative</code> - debugger shows cummulative messages.
     * </ul>
     */
    public static void main(String[] args) {
        for(int i=0; i < args.length; i++) {
            if("-sleep".equals(args[i])) {
                SLEEP_BETWEEN_SENDING=true;
                if(!(i < args.length - 1))
                    throw new RuntimeException("You have to specify sleep time");

                try {
                    SLEEP_TIME=Integer.parseInt(args[++i]);
                }
                catch(NumberFormatException nfex) {
                    throw new RuntimeException("Cannot parse sleep time");
                }

                continue;
            }
            else if("-msg_num".equals(args[i])) {
                if(!(i < args.length - 1))
                    throw new RuntimeException("You have to specify messages number");

                try {
                    MESSAGE_NUMBER=Integer.parseInt(args[++i]);
                }
                catch(NumberFormatException nfex) {
                    throw new RuntimeException("Cannot parse messages number");
                }

                continue;
            }
            else if("-loopback".equals(args[i])) {
                LOOPBACK=true;

                continue;
            }
            else if("-debug".equals(args[i])) {
                USE_DEBUGGER=true;

                continue;
            }
            else if("-cummulative".equals(args[i])) {
                CUMMULATIVE=true;
                continue;
            }
            else if("-help".equals(args[i])) {
                help();
                return;
            }
        }

        junit.textui.TestRunner.run(MessageLoadTest.class);
    }

    static void help() {
        System.out.println("MessageLoadTest [-help] [-sleep <sleep time between sends (ms)>] " +
                " [-msg_num <number of msgs to send>] [-debug [-cummulative]] [-loopback]");
    }

}
