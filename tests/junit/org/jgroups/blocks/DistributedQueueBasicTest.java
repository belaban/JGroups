// $Id: DistributedQueueBasicTest.java,v 1.4 2004/05/03 14:53:39 belaban Exp $

package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.log4j.Logger;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;

public class DistributedQueueBasicTest extends TestCase implements MessageListener
{

    static Logger logger = Logger.getLogger(DistributedQueueBasicTest.class.getName());
    String props;

    public DistributedQueueBasicTest(String testName)
    {
        super(testName);
    }

    public static Test suite()
    {
        return new TestSuite(DistributedQueueBasicTest.class);
    }

    protected DistributedQueue tQueue1;
    protected DistributedQueue tQueue2;

    protected static boolean logConfigured;

    public void setUp() throws Exception
    {

        //		String props =
        //			"UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;"
        //				+ "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):"
        //				+ "PING(timeout=2000;num_initial_members=5):"
        //				+ "FD_SOCK:"
        //				+ "VERIFY_SUSPECT(timeout=1500):"
        //				+ "UNICAST(timeout=5000):"
        //				+ "FRAG(frag_size=8192;down_thread=false;up_thread=false):"
        //				+ "TOTAL_TOKEN(block_sending=50;unblock_sending=10):"
        //				+ "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;"
        //				+ "shun=false;print_local_addr=true):"
        //				+ "STATE_TRANSFER:"
        //				+ "QUEUE";

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
        if(tQueue2 != null)
            tQueue2.stop();

        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException ex)
        {
        }
        if(tQueue1 != null)
            tQueue1.stop();
    }

    class FetchTask implements Runnable
    {
        protected DistributedQueue queue;
        protected String name;
        String r;
        protected boolean finished;

        public FetchTask(String name, DistributedQueue q)
        {
            queue = q;
            this.name = name;
            finished = false;
        }

        public void run()
        {
            r = (String) queue.remove();
            logger.debug("Remove from " + name + ":" + r);
            finished = true;
        }

        public String getResult()
        {
            return r;
        }

        public boolean finished()
        {
            return finished;
        }
    }

    public void test() throws Exception
    {

		tQueue1 = new DistributedQueue("basic", null, props, 5000);

		// give some time for the channel to become a coordinator
		try
		{
			Thread.sleep(1000);
		}
		catch (Exception ex)
		{
		}

		tQueue2 = new DistributedQueue("basic", null, props, 5000);

		// give some time for the channel to become a coordinator
		try
		{
			Thread.sleep(1000);
		}
		catch (Exception ex)
		{
		}


        FetchTask t1 = new FetchTask("Queue1", tQueue1);
        FetchTask t2 = new FetchTask("Queue2", tQueue2);
        Thread rTask1 = new Thread(t1);
        Thread rTask2 = new Thread(t2);

        String obj1 = "object1";
        String obj2 = "object2";
        String obj3 = "object3";
        tQueue1.add(obj1);
        tQueue1.add(obj2);
        tQueue1.add(obj3);

        rTask1.start();
        rTask2.start();

        while (!(t1.finished() && t2.finished()))
        {
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException ex)
            {
            }
        }

        String obj = (String) tQueue2.remove();
        assertEquals(obj3, obj);
        logger.debug("Removed from Queue2:" + obj);

        obj = (String) tQueue2.remove();
        assertNull(obj);
        logger.debug("Removed from Queue2:" + obj);

        obj = (String) tQueue2.remove();
        assertNull(obj);
        logger.debug("Removed from Queue2:" + obj);
    }

    public void receive(Message msg)
    {
        logger.debug("Received msg: " + msg);
    }

    public byte[] getState()
    { // only called if channel option GET_STATE_EVENTS is set to true
        return null;
    }

    public void setState(byte[] state)
    {
    }

    public void testWithPullPushAdapter()
    {
        try
        {
            JChannel channel1 = new JChannel(props);
            channel1.connect("PullPushTest");
            PullPushAdapter adapter1 = new PullPushAdapter(channel1);
            adapter1.setListener(this);

			tQueue1 = new DistributedQueue(adapter1,"basic");

			// give some time for the channel to become a coordinator
			try
			{
				Thread.sleep(1000);
			}
			catch (Exception ex)
			{
			}
			tQueue1.start(1000);

			JChannel channel2 = new JChannel(props);
			channel2.connect("PullPushTest");
			PullPushAdapter adapter2 = new PullPushAdapter(channel2);
			adapter2.setListener(this);

			tQueue2 = new DistributedQueue(adapter2, "basic");
			tQueue2.start(2000);

			FetchTask t1 = new FetchTask("Queue1", tQueue1);
			FetchTask t2 = new FetchTask("Queue2", tQueue2);
			Thread rTask1 = new Thread(t1);
			Thread rTask2 = new Thread(t2);

			String obj1 = "object1";
			String obj2 = "object2";
			String obj3 = "object3";
			tQueue1.add(obj1);
			tQueue1.add(obj2);
			tQueue1.add(obj3);

			rTask1.start();
			rTask2.start();

			while (!(t1.finished() && t2.finished()))
			{
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException ex)
				{
				}
			}

			String obj = (String) tQueue2.remove();
			assertEquals(obj3, obj);
			logger.debug("Removed from Queue2:" + obj);

			obj = (String) tQueue2.remove();
			assertNull(obj);
			logger.debug("Removed from Queue2:" + obj);

			obj = (String) tQueue2.remove();
			assertNull(obj);
			logger.debug("Removed from Queue2:" + obj);

        }
        catch (ChannelException e)
        {
            logger.error("Cannot initialize", e);
        }
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(suite());
    }
}
