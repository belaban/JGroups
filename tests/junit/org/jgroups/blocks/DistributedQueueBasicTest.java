// $Id: DistributedQueueBasicTest.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.jgroups.log.Trace;

public class DistributedQueueBasicTest extends TestCase
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


        props="UDP(mcast_recv_buf_size=80000;mcast_send_buf_size=150000;mcast_port=45566;" +
                "mcast_addr=228.8.8.8;ip_ttl=32):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "UNICAST(timeout=600,1200,2000,2500):" +
                "FRAG(frag_size=8096;down_thread=false;up_thread=false):" +
                "TOTAL_TOKEN(unblock_sending=10;block_sending=50):" +
                "pbcast.GMS(print_local_addr=true;join_timeout=3000;join_retry_timeout=2000;shun=true):" +
                "STATE_TRANSFER:" +
                "QUEUE";

		Trace.init();

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
	}

	public void tearDown() throws Exception
	{
		tQueue2.stop();

		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException ex)
		{
		}

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

	public static void main(String[] args)
	{
		junit.textui.TestRunner.run(suite());
	}
}
