// $Id: DistributedHashtableTest.java,v 1.2 2004/03/30 06:47:30 belaban Exp $
package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.log4j.Logger;

public class DistributedHashtableTest extends TestCase
{

	final int NUM_ITEMS = 10;
	static Logger logger = Logger.getLogger(DistributedHashtableTest.class.getName());

	public DistributedHashtableTest(String testName)
	{
		super(testName);
	}

	public static Test suite()
	{
		return new TestSuite(DistributedHashtableTest.class);
	}

	protected DistributedHashtable queue1;
	protected DistributedHashtable queue2;
	protected DistributedHashtable queue3;

	public void setUp() throws Exception
	{

		String props =
			"UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;"
				+ "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):"
				+ "PING(timeout=2000;num_initial_members=5):"
				+ "MERGE2(min_interval=5000;max_interval=10000):"
				+ "FD_SOCK:"
				+ "VERIFY_SUSPECT(timeout=1500):"
				+ "UNICAST(timeout=5000):"
				+ "FRAG(frag_size=8192;down_thread=false;up_thread=false):"
				+ "TOTAL_TOKEN(block_sending=50;unblock_sending=10):"
				+ "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;"
				+ "shun=false;print_local_addr=true):"
				+ "STATE_TRANSFER:"
				+ "QUEUE";
				


		queue1 = new DistributedHashtable("testing", null, props, 10000);

		// give some time for the channel to become a coordinator
		try
		{
			Thread.sleep(1000);
		}
		catch (Exception ex)
		{
		}

		queue2 = new DistributedHashtable("testing", null, props, 10000);

		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException ex)
		{
		}

		queue3 = new DistributedHashtable("testing", null, props, 10000);

		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException ex)
		{
		}
	}

	public void tearDown() throws Exception
	{
		queue1.stop();
		queue2.stop();
		queue3.stop();
	}

	class PutTask implements Runnable
	{
		protected DistributedHashtable queue;
		protected String name;
		protected boolean finished;

		public PutTask(String name, DistributedHashtable q)
		{
			queue = q;
			this.name = name;
			finished = false;
		}

		public void run()
		{
			Boolean yes = new Boolean(true);
			for (int i = 0; i < NUM_ITEMS; i++)
			{
					queue.put(name + "_" + i, yes);
			}
			finished = true;
		}

		public boolean finished()
		{
			return finished;
		}
	}


	public void testConcurrent() throws Exception
	{
		PutTask t1 = new PutTask("Queue1", queue1);
		PutTask t2 = new PutTask("Queue2", queue2);
		PutTask t3 = new PutTask("Queue3", queue3);
		Thread rTask1 = new Thread(t1);
		Thread rTask2 = new Thread(t2);
		Thread rTask3 = new Thread(t3);

		// DistributedQueueProtocolObserver po1 = new DistributedQueueProtocolObserver(queue1);
		// DistributedQueueProtocolObserver po2 = new DistributedQueueProtocolObserver(queue2);
		// DistributedQueueProtocolObserver po3 = new DistributedQueueProtocolObserver(queue3);
		
		rTask1.start();
		rTask2.start();
		rTask3.start();

		while (!t1.finished() || !t2.finished() || !t3.finished())
		{
			try
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException ex)
			{
			}
		}

		assertEquals(queue1.size(), queue2.size());
		assertEquals(queue1.size(), queue3.size());

		assertEquals(queue1.keySet(), queue2.keySet());
		assertEquals(queue1.keySet(), queue3.keySet());
	}
	
	public static void main(String[] args)
	{
		junit.textui.TestRunner.run(suite());
	}
}
