// $Id: DistributedQueueConcurrencyTest.java,v 1.4 2003/11/24 16:37:13 rds13 Exp $

package org.jgroups.blocks;

import java.util.Vector;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

/**
 * This class setup multiple DistributedQueue, some handles by a reader thread
 * others by a writer thread.
 * @author Romuald du Song
 */
public class DistributedQueueConcurrencyTest extends TestCase implements ICounter
{
    protected static int items = 0;

    final int NUM_CLIENTS = 5;
    final int NUM_ITEMS = 10;
    final int REPEAT_TEST= 1;
    final int STATE_TRANSFER_TIMEOUT = 4000;

    static Logger logger = Logger.getLogger(DistributedQueueConcurrencyTest.class.getName());
    String props;

    public DistributedQueueConcurrencyTest(String testName)
    {
        super(testName);
    }

    public static Test suite()
    {
        return new TestSuite(DistributedQueueConcurrencyTest.class);
    }

    protected DistributedQueue queue1;
    protected DistributedQueue queue2;
    protected DistributedQueue queue3;

    protected DistributedQueue queue;
    protected DistributedQueue queuePut;

    public void setUp() throws Exception
    {

               props =
                   "UDP(mcast_recv_buf_size=80000;mcast_send_buf_size=150000;mcast_port=45566;"
                       + "mcast_addr=228.8.8.8;ip_ttl=32):"
                       + "PING(timeout=2000;num_initial_members=5):"
                       + "MERGE2(min_interval=5000;max_interval=10000):"
                       + "FD_SOCK:"
                       + "VERIFY_SUSPECT(timeout=1500):"
                       + "UNICAST(timeout=600,1200,2000,2500):"
                       + "FRAG(frag_size=8192;down_thread=false;up_thread=false):"
                       + "TOTAL_TOKEN(block_sending=50;unblock_sending=10):"
                       + "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;"
                       + "shun=true;print_local_addr=true):"
                       + "STATE_TRANSFER:"
                       + "QUEUE";

		/*
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
        */

        Trace.init();

        queue1 = new DistributedQueue("concurency", null, props, STATE_TRANSFER_TIMEOUT);

        // give some time for the channel to become a coordinator
		Util.sleep(1000);

        queue2 = new DistributedQueue("concurency", null, props, STATE_TRANSFER_TIMEOUT);
		Util.sleep(1000);

    }

    public void tearDown() throws Exception
    {
		logger.info("start tearDown()");
        queue1.stop();
		logger.info("intermediate tearDown()");
        queue2.stop();
		logger.info("end tearDown()");
		Util.sleep(1000);
    }

	public synchronized int increment()
	{
		return ++items;
	}

	public int getValue()
	{
		return items;
	}

    /**
     * Two reader-thread are launched with nothing in the queue,
     * a little time after one writer-thread is launched.
     * When the two readers have read all the elements put in
     * the queue, we compare the elements put by the writer
     * to the elements output by the readers.
     * @throws Exception
     */
    public void testConcurrentOneItem() throws Exception
    {
        logger.info("start testConcurrentOneItem");
        DistributedQueueReadTask t1 = new DistributedQueueReadTask("Queue1", queue1, this, 1, 5000);
        DistributedQueueReadTask t2 = new DistributedQueueReadTask("Queue2", queue2, this, 1, 5000);
        Thread rTask1 = new Thread(t1);
        Thread rTask2 = new Thread(t2);

        rTask1.start();
        rTask2.start();

		Util.sleep(6000);

        queue3 = new DistributedQueue("concurency", null, props, STATE_TRANSFER_TIMEOUT);

        DistributedQueuePutTask t3 = new DistributedQueuePutTask("Queue3", queue3, 1, 0);
        Thread rTask3 = new Thread(t3);

        rTask3.start();
        while (!t3.finished())
        {
			Util.sleep(1000);
        }

        while (!t1.finished() && !t2.finished())
        {
			Util.sleep(1000);
        }

        assertEquals(0, queue1.size());
        assertEquals(0, queue2.size());
        assertEquals(0, queue3.size());

        int total = t1.getContent().size() + t2.getContent().size();
        assertEquals(1, total);
        assertEquals(1, t3.getContent().size());

        Vector totalContent = t1.getContent();
        totalContent.addAll(t2.getContent());
        checkContents(t3.getContent(), totalContent);

        queue3.stop();
        logger.info("end testConcurrentOneItem");
    }

    /**
     * Two reader-thread are launched with nothing in the queue,
     * a little time after one writer-thread is launched.
     * When the two readers have read all the elements put in
     * the queue, we compare input elements by the writer
     * to output elements by the readers.
     * @throws Exception
     */
    public void testConcurrentMultipleItems() throws Exception
    {
        for (int i = 0; i < REPEAT_TEST; i++)
        {
            _concurrentMultipleItems(i);
        }
    }

    protected void _concurrentMultipleItems(int i) throws Exception
    {
        logger.info("start testConcurrentMultipleItems " + i + " run");
        items = 0;

        DistributedQueueReadTask t1 = new DistributedQueueReadTask("Queue1", queue1, this, NUM_ITEMS, 5000);
        DistributedQueueReadTask t2 = new DistributedQueueReadTask("Queue2", queue2, this, NUM_ITEMS, 5000);
        Thread rTask1 = new Thread(t1);
        Thread rTask2 = new Thread(t2);

        rTask1.start();
        rTask2.start();

        Util.sleep(6000);

        queue3 = new DistributedQueue("concurency", null, props, STATE_TRANSFER_TIMEOUT);
        DistributedQueuePutTask t3 = new DistributedQueuePutTask("Queue3", queue3, NUM_ITEMS, 6000);
        Thread rTask3 = new Thread(t3);

        rTask3.start();
        while (!t3.finished())
        {
            Util.sleep(1000);
        }

        while (!t1.finished() || !t2.finished())
        {
            Util.sleep(1000);
        }

        if (queue1.size() > 0)
            logger.debug("Queue1:" + queue1.toString());
        if (queue2.size() > 0)
            logger.debug("Queue2:" + queue2.toString());
        if (queue3.size() > 0)
            logger.debug("Queue3:" + queue3.toString());
            
        assertEquals(0, queue1.size());
        assertEquals(0, queue2.size());
        assertEquals(0, queue3.size());

        int total = t1.getContent().size() + t2.getContent().size();
        assertEquals(NUM_ITEMS, total);
        assertEquals(NUM_ITEMS, t3.getContent().size());

        Vector totalContent = t1.getContent();
        totalContent.addAll(t2.getContent());
        checkContents(t3.getContent(), totalContent);

        queue3.stop();
        queue3 = null;
        logger.info("end testConcurrentMultipleItems");
    }

    protected void checkContents(Vector q1, Vector q2)
    {
        assertEquals(q1.size(), q2.size());
		assertTrue(q1.containsAll(q2));
		assertTrue(q2.containsAll(q1));
    }
    
	public void testMultipleReaderOneWriter() throws Exception
	{
		logger.info("start testMultipleReaderOneWriter");
		// reset counter
		items = 0;

		Vector queues = new Vector();
		for (int i = 0; i < NUM_CLIENTS; i++)
		{
			queue = new DistributedQueue("crashme", null, props, STATE_TRANSFER_TIMEOUT);
			// give some time for the channel to become a coordinator
			Util.sleep(500);
			queues.add(queue);
		}
		
		Vector vTask = new Vector();
		for (int i = 0; i < NUM_CLIENTS; i++)
		{
			queue = (DistributedQueue) queues.elementAt(i);
			DistributedQueueReadTask t = new DistributedQueueReadTask("Queue" + i, queue, this, NUM_ITEMS, 5000);
			vTask.add(t);
			Thread task = new Thread(t);
			task.start();
			Util.sleep(500);
		}


		queuePut = new DistributedQueue("crashme", null, props, STATE_TRANSFER_TIMEOUT);
		Util.sleep(1000);

		DistributedQueuePutTask putTask = new DistributedQueuePutTask("PutQueue", queuePut, NUM_ITEMS, 200);
		Thread rTask3 = new Thread(putTask);

		rTask3.start();
		while (!putTask.finished())
		{
			Util.sleep(1000);
		}

		boolean finished = false;
		while (!finished)
		{
			finished = true;
			for (int i = 0; i < NUM_CLIENTS; i++)
			{
				DistributedQueueReadTask reader = (DistributedQueueReadTask) vTask.elementAt(i);
				if (!reader.finished())
				{
					finished = false;
					break;
				}
			}
			Util.sleep(1000);
		}

		Vector totalContent = new Vector();
		for (int i = 0; i < NUM_CLIENTS; i++)
		{
			queue = (DistributedQueue) queues.elementAt(i);
			assertEquals(0, queue.size());
			DistributedQueueReadTask t = (DistributedQueueReadTask) vTask.elementAt(i);
			totalContent.addAll(t.getContent());
			logger.debug("New total:"+totalContent.size());
		}

		assertEquals(0, queuePut.size());

		assertEquals(NUM_ITEMS, totalContent.size());
		assertEquals(NUM_ITEMS, putTask.getContent().size());
		checkContents(putTask.getContent(), totalContent);

		queuePut.stop();
		
		for (int i = 0; i < NUM_CLIENTS; i++)
		{
			queue = (DistributedQueue) queues.elementAt(i);
			queue.stop();
		}

		logger.info("end testMultipleReaderOneWriter");
	}


    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(suite());
    }
}
