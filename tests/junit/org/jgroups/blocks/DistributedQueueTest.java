// $Id: DistributedQueueTest.java,v 1.13 2008/04/08 08:29:43 belaban Exp $

package org.jgroups.blocks;



import org.testng.annotations.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.tests.ChannelTestBase;

import java.util.Vector;

public class DistributedQueueTest extends ChannelTestBase
{

	final int NUM_ITEMS = 10;
	static Log logger = LogFactory.getLog(DistributedQueueTest.class);
    String props;


	protected DistributedQueue queue1;
	protected DistributedQueue queue2;
	protected DistributedQueue queue3;

	public void setUp() throws Exception
	{

        ;
        props="UDP(mcast_recv_buf_size=80000;mcast_send_buf_size=150000;mcast_port=45566;" +
                "mcast_addr=228.8.8.8;ip_ttl=32):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "UNICAST(timeout=600,1200,2000,2500):" +
                "FRAG(frag_size=8096):" +
                "TOTAL_TOKEN(unblock_sending=10;block_sending=50):" +
                "pbcast.GMS(print_local_addr=true;join_timeout=3000;shun=true):" +
                "STATE_TRANSFER:" +
                "QUEUE";



		queue1 = new DistributedQueue("testing", null, props, 5000);
        log("created queue1");

        // give some time for the channel to become a coordinator
		try
		{
			Thread.sleep(1000);
		}
		catch (Exception ex)
		{
		}

		queue2 = new DistributedQueue("testing", null, props, 5000);
        log("created queue2");

        try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException ex)
		{
		}

		queue3 = new DistributedQueue("testing", null, props, 5000);
        log("created queue3");

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
        ;
        log("stopping queue1");
        queue1.stop();
        log("stopped queue1");

        log("stopping queue2");
        queue2.stop();
        log("stopped queue2");

        log("stopping queue3");
        queue3.stop();
        log("stopped queue3");
    }

    void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "]: " + msg);
    }

    class PutTask implements Runnable
	{
		protected DistributedQueue queue;
		protected String name;
		protected boolean finished;

		public PutTask(String name, DistributedQueue q)
		{
			queue = q;
			this.name = name;
			finished = false;
		}

		public void run()
		{
			for (int i = 0; i < NUM_ITEMS; i++)
			{
				queue.add(name + '_' + i);
			}
			finished = true;
            log("added " + NUM_ITEMS + " elements - done");
        }

		public boolean finished()
		{
			return finished;
		}
	}


	public void testMultipleWriter() throws Exception
	{
		PutTask t1 = new PutTask("Queue1", queue1);
		PutTask t2 = new PutTask("Queue2", queue2);
		PutTask t3 = new PutTask("Queue3", queue3);
		Thread rTask1 = new Thread(t1);
		Thread rTask2 = new Thread(t2);
		Thread rTask3 = new Thread(t3);

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

		checkContents(queue1.getContents(), queue2.getContents());
		checkContents(queue1.getContents(), queue3.getContents());
	}

	protected void checkContents(Vector q1, Vector q2)
	{
		for (int i = 0; i < q1.size(); i++)
		{
			Object e1 = q1.elementAt(i);
			Object e2 = q2.elementAt(i);
			boolean t = e1.equals(e2);
			if (!t)
			{
				logger.error("Data order differs :" + e1 + "!=" + e2);
			} else
			logger.debug("Data order ok :" + e1 + "==" + e2);
			assertTrue(e1.equals(e2));
		}
	}

}
