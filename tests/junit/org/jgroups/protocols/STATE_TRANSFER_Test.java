package org.jgroups.protocols;


import static java.util.concurrent.TimeUnit.SECONDS;

import org.jgroups.*;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;

/**
 * It's an attemp to setup Junit test case template for Protocol regression. <p>
 * Two "processes" are started, and the coord. keeps sending msg of a counter. The 2nd
 * process joins the grp and get the state from the coordinator. The subsequent msgs
 * after the setState will be validated to ensure the total ordering of msg delivery. <p>
 * This should cover the fix introduced by rev. 1.12
 * @author Wenbo Zhu
 * @version $Id: STATE_TRANSFER_Test.java,v 1.20 2009/06/17 16:35:48 belaban Exp $
 */
@Test(groups={Global.STACK_DEPENDENT,"known-failures"})
public class STATE_TRANSFER_Test extends ChannelTestBase {
    public static final String GROUP_NAME="STATE_TRANSFER_Test";
    private Coordinator coord;



    @BeforeMethod
    protected void setUp() throws Exception {      
        coord=new Coordinator();
        coord.recvLoop();
        coord.sendLoop();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        coord.stop();
        coord=null;
    }

    class Coordinator extends ChannelListenerAdapter {

        private JChannel channel=null;
        private int cnt=0;  // the state
        private volatile boolean closed=false;
        String getProps() {
            return channel.getProperties();
        }

        public JChannel getChannel() {
            return channel;
        }

        protected Coordinator() throws Exception {
            channel=createChannel(true);
            channel.setOpt(Channel.LOCAL, Boolean.FALSE);
            channel.addChannelListener(this);
            channel.connect(GROUP_NAME);
        }


        public void recvLoop() throws Exception {
            Thread task=new Thread(new Runnable() {
                public void run() {
                    Object tmp;
                    while(!closed) {
                        try {
                            tmp=channel.receive(0);
                            if(tmp instanceof GetStateEvent) {
                                synchronized(Coordinator.this) {
                                    // System.err.println("--  GetStateEvent, cnt=" + cnt);
                                    channel.returnState(Util.objectToByteBuffer(new Integer(cnt)));
                                }
                            }
                        }
                        catch(ChannelNotConnectedException not) {
                            break;
                        }
                        catch(ChannelClosedException closed) {
                            break;
                        }
                        catch(Exception e) {
                            System.err.println(e);
                        }
                    }
                }
            });
            task.start();
        }

        public void sendLoop() throws Exception {
            Thread task=new Thread(new Runnable() {

                public void run() {
                    while(!closed) {
                        try {
                            synchronized(Coordinator.this) {
                                channel.send(null, null, new Integer(++cnt));
                                // System.err.println("send cnt=" + cnt);
                            }
                            Thread.sleep(1000);
                        }
                        catch(ChannelNotConnectedException not) {
                            break;
                        }
                        catch(ChannelClosedException closed) {
                            break;
                        }
                        catch(Exception e) {
                            System.err.println(e);
                        }
                    }
                }
            });
            task.start();
        }

        public void stop() {
            closed=true;
            channel.close();
        }
    }

    public void testBasicStateSync() throws Exception {
        Channel channel= null;
        int timeout=60; //seconds
        int counter=0;
        try {
            channel = createChannel(coord.getChannel());
            channel.setOpt(Channel.LOCAL, Boolean.FALSE);

            channel.connect(GROUP_NAME);

            Thread.sleep(1000);

            boolean join=false;
            join=channel.getState(null, 100000l);
            assertTrue(join);

            Object tmp;
            int cnt=-1;
            for(;counter < timeout;SECONDS.sleep(1),counter++) {
                try {
                    tmp=channel.receive(0);
                    if(tmp instanceof SetStateEvent) {
                        cnt=((Integer)Util.objectFromByteBuffer(((SetStateEvent)tmp).getArg())).intValue();
                        // System.err.println("--  SetStateEvent, cnt=" + cnt);
                        continue;
                    }
                    if(tmp instanceof Message) {
                        if(cnt != -1) {
                            int msg=((Integer)((Message)tmp).getObject()).intValue();
                            assertEquals(cnt, msg - 1);
                            break; // done
                        }
                    }
                }
                catch(ChannelNotConnectedException not) {
                    break;
                }
                catch(ChannelClosedException closed) {
                    break;
                }
                catch(Exception e) {
                    System.err.println(e);
                }
            }
        }
        finally {
            channel.close();
            assertTrue("Timeout reached", counter < timeout);
        }
    }
}
