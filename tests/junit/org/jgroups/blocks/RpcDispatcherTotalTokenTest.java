// $Id: RpcDispatcherTotalTokenTest.java,v 1.1 2003/10/21 08:31:08 rds13 Exp $

package org.jgroups.blocks;

import java.util.Vector;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.jgroups.JChannel;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

/**
 * This test focus on whether RpcDispatcher is respecting total order.
 * It is based on org.jgroups.tests.RpcDispatcherTotalTokenTest.
 * Test for RpcDispatcher (see also MessageDispatcher). A remote method (print()) is group-invoked
 * periodically. The method is defined in each instance and is invoked whenever a remote method call
 * is received. The callee (although in this example, each callee is also a caller (peer principle))
 * has to define the public methods, and the caller uses one of the callRemoteMethods() methods to
 * invoke a remote method. CallRemoteMethods uses the core reflection API to lookup and dispatch
 * methods.
 * @author Romuald du Song
 */
public class RpcDispatcherTotalTokenTest extends TestCase
{
    String props = null;

    final int NUM_ITEMS = 10;
    static Logger logger = Logger.getLogger(RpcDispatcherTotalTokenTest.class.getName());

    public RpcDispatcherTotalTokenTest(String testName)
    {
        super(testName);
    }

    public class DispatcherTask implements Runnable
    {
        protected String taskName;
        protected boolean finished;
        JChannel channel;
        RpcDispatcher disp;
        RspList rsp_list;
        int base;
        boolean bTest;

        public DispatcherTask(String taskName, String jgProps) throws Exception
        {
            this.taskName = taskName;
            this.base = 0;
            finished = false;
            bTest = true;
            channel = new JChannel(jgProps);
            disp = new RpcDispatcher(channel, null, null, this);
            channel.connect("RpcDispatcherTestGroup");
        }

        public synchronized int print(String tag) throws Exception
        {
            return base++;
        }

        public JChannel getChannel()
        {
            return channel;
        }

        public boolean finished()
        {
            return finished;
        }

        public boolean result()
        {
            return bTest;
        }

        protected boolean checkResult(RspList rsp)
        {
            boolean result = true;
            Vector results = rsp.getResults();
            Object retval = null;
            if (results.size() > 0 && rsp.numSuspectedMembers() == 0)
            {
                retval = results.elementAt(0);
                for (int i = 1; i < results.size(); i++)
                {
                    Object data = results.elementAt(i);
                    boolean test = data.equals(retval);
                    if (!test)
                    {
                        logger.error(
                            "Task "
                                + taskName
                                + ":Reference value differs from returned value "
                                + retval
                                + " != "
                                + data);
                        result = false;
                    }
                }
            }
            return result;
        }

        public void run()
        {
            logger.debug("Task " + taskName + " View is :" + channel.getView());

            for (int i = 0; i < NUM_ITEMS && bTest; i++)
            {
                Util.sleep(100);

                try
                {
                    MethodCall call =
                        new MethodCall(
                            "print",
                            new Object[] { new String(taskName + "_" + i)},
                            new String[] { String.class.getName()});
                    rsp_list = disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
                    logger.debug("Task " + taskName + " Responses: " + rsp_list);
                    bTest = checkResult(rsp_list);
                }
                catch (Exception ex)
                {
                    logger.error("Unexpected error", ex);
                }

            }

            finished = true;
        }

        public void close()
        {
            try
            {
                logger.debug("Stopping dispatcher");
                disp.stop();
                logger.debug("Stopping dispatcher: -- done");

                Util.sleep(200);
                logger.debug("Closing channel");
                channel.close();
                logger.debug("Closing channel: -- done");
            }
            catch (Exception e)
            {
                logger.error("Unexpected error", e);
            }
        }
    }

    public void setUp() throws Exception
    {
        props =
            "UDP(mcast_recv_buf_size=80000;mcast_send_buf_size=150000;mcast_port=45566;"
                + "mcast_addr=228.8.8.8;ip_ttl=32):"
                + "PING(timeout=2000;num_initial_members=3):"
                + "FD_SOCK:"
                + "VERIFY_SUSPECT(timeout=1500):"
                + "UNICAST(timeout=600,1200,2000,2500):"
                + "FRAG(frag_size=8096;down_thread=false;up_thread=false):"
                + "TOTAL_TOKEN(unblock_sending=10;block_sending=50):"
                + "pbcast.GMS(print_local_addr=true;join_timeout=3000;join_retry_timeout=2000;shun=true):"
                + "STATE_TRANSFER:"
                + "QUEUE";
    }

    public void tearDown() throws Exception
    {
    }

    public void test() throws Exception
    {
        DispatcherTask t1 = new DispatcherTask("Task1", props);
        DispatcherTask t2 = new DispatcherTask("Task2", props);
        DispatcherTask t3 = new DispatcherTask("Task3", props);

        Thread rTask1 = new Thread(t1);
        Thread rTask2 = new Thread(t2);
        Thread rTask3 = new Thread(t3);

        TotalTokenProtocolObserver po1 = new TotalTokenProtocolObserver(t1.getChannel());
        TotalTokenProtocolObserver po2 = new TotalTokenProtocolObserver(t2.getChannel());
        TotalTokenProtocolObserver po3 = new TotalTokenProtocolObserver(t3.getChannel());

        Util.sleep(1000);

        rTask1.start();
        rTask2.start();
        rTask3.start();

        while (!t1.finished() || !t2.finished() || !t3.finished())
        {
            Util.sleep(1000);
        }

        t1.close();
        t2.close();
        t3.close();

        assertTrue(t1.result());
        assertTrue(t2.result());
        assertTrue(t3.result());
    }

    public static Test suite()
    {
        return new TestSuite(RpcDispatcherTotalTokenTest.class);
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(suite());
    }

}
