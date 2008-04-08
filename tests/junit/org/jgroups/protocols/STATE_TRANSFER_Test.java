// $Id: STATE_TRANSFER_Test.java,v 1.10 2008/04/08 07:19:12 belaban Exp $
package org.jgroups.protocols;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelException;
import org.jgroups.ChannelListener;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.ExitEvent;
import org.jgroups.GetStateEvent;
import org.jgroups.JChannel;
import org.jgroups.SetStateEvent;
import org.jgroups.Message;
import org.jgroups.util.Util;

/**
 * It's an attemp to setup Junit test case template for Protocol regression. <p>
 * Two "processes" are started, and the coord. keeps sending msg of a counter. The 2nd
 * process joins the grp and get the state from the coordinator. The subsequent msgs
 * after the setState will be validated to ensure the total ordering of msg delivery. <p>
 * This should cover the fix introduced by rev. 1.12
 *
 * @author Wenbo Zhu
 * @version 1.0
 */
public class STATE_TRANSFER_Test extends TestCase {

   public final static String CHANNEL_PROPS ="udp.xml";

   public static final String GROUP_NAME = "jgroups.TEST_GROUP";

   private Coordinator coord;

   public STATE_TRANSFER_Test(String testName) {
      super(testName);
   }

   protected void setUp() throws Exception {
      ;

      System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
      System.setProperty("org.apache.commons.logging.simplelog.defaultlog", "error");

      coord = new Coordinator();
      coord.recvLoop();
      coord.sendLoop();
   }

   protected void tearDown() throws Exception {
      ;

      coord.stop();
      coord = null;
   }

   static class Coordinator implements ChannelListener {

      private JChannel channel = null;
      private int cnt = 0;  // the state
      private volatile boolean closed = false;

      protected Coordinator() throws ChannelException {

         channel = new JChannel(CHANNEL_PROPS);
         channel.setOpt(Channel.LOCAL, Boolean.FALSE);
         channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
         channel.addChannelListener(this);
         channel.connect(GROUP_NAME);
      }

      public void channelConnected(Channel channel) {
      }

      public void channelDisconnected(Channel channel) {
      }

      public void channelClosed(Channel channel) {
      }

      public void channelShunned() {
      }

      public void channelReconnected(Address addr) {     // n/a. now
      }

      public void recvLoop() throws Exception {
         Thread task = new Thread(new Runnable() {
            public void run() {
               Object tmp;
               while (! closed) {
                  try {
                     tmp = channel.receive(0);
                     if (tmp instanceof ExitEvent) {
                        System.err.println("-- received EXIT, waiting for ChannelReconnected callback");
                        break;
                     }
                     if (tmp instanceof GetStateEvent) {
                        synchronized (Coordinator.this) {
                           System.err.println("--  GetStateEvent, cnt=" + cnt);
                           channel.returnState(Util.objectToByteBuffer(new Integer(cnt)));
                        }
                     }
                  } catch (ChannelNotConnectedException not) {
                     break;
                  } catch (ChannelClosedException closed) {
                     break;
                  } catch (Exception e) {
                     System.err.println(e);
                  }
               }
            }
         });
         task.start();
      }

      public void sendLoop() throws Exception {
         Thread task = new Thread(new Runnable() {

            public void run() {
               while (! closed) {
                  try {
                     synchronized (Coordinator.this) {
                        channel.send(null, null, new Integer(++cnt));
                        System.err.println("send cnt=" + cnt);
                     }
                     Thread.sleep(1000);
                  } catch (ChannelNotConnectedException not) {
                     break;
                  } catch (ChannelClosedException closed) {
                     break;
                  } catch (Exception e) {
                     System.err.println(e);
                  }
               }
            }
         });
         task.start();
      }

      public void stop() {
         closed = true;
         channel.close();
      }
   }

   public void testBasicStateSync() throws Exception {

      Channel channel = new JChannel(CHANNEL_PROPS);
      channel.setOpt(Channel.LOCAL, Boolean.FALSE);

      channel.connect(GROUP_NAME);

      Thread.sleep(1000);

      boolean join = false;
      join = channel.getState(null, 100000l);
      assertTrue(join);

      Object tmp;
      int cnt = -1;
      while (true) {
         try {
            tmp = channel.receive(0);
            if (tmp instanceof ExitEvent) {
               break;
            }
            if (tmp instanceof SetStateEvent) {
               cnt = ((Integer) Util.objectFromByteBuffer(((SetStateEvent) tmp).getArg())).intValue();
               System.err.println("--  SetStateEvent, cnt=" + cnt);
               continue;
            }
            if ( tmp instanceof Message ) {
               if (cnt != -1) {
                  int msg = ((Integer) ((Message) tmp).getObject()).intValue();
                  assertEquals(cnt, msg - 1);
                  break;  // done
               }
            }
         } catch (ChannelNotConnectedException not) {
            break;
         } catch (ChannelClosedException closed) {
            break;
         } catch (Exception e) {
            System.err.println(e);
         }
      }

      channel.close();
   }


   public static Test suite() {
      return new TestSuite(STATE_TRANSFER_Test.class);
   }

   public static void main(String[] args) {
      junit.textui.TestRunner.run(suite());
   }

}
