// $Id: ChannelMonoTest.java,v 1.6 2003/09/24 12:09:55 rds13 Exp $

package org.jgroups.tests;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.util.Util;

/*
 * Tests operations on one Channel instance with various threads reading/writing :
 * - one writer, n readers with or without timeout
 * - one reader, n writers
 * @author rds13
 */
public class ChannelMonoTest extends TestCase
{
	private Channel channel = null;
	static Logger logger = Logger.getLogger(ChannelMonoTest.class);
	String channelName = "ChannelLg4jTest";

	public ChannelMonoTest(String Name_)
	{
		super(Name_);
	}

	public void setUp()
	{
		try
		{
			channel = new JChannel(null);
			channel.connect(channelName);
		}
		catch (ChannelException e)
		{
			logger.error("Channel init problem", e);
		}
	}

	public void tearDown()
	{
		channel.disconnect();
		channel.close();
		channel = null;
	}

	public void testChannel()
	{
		assertNotNull(channel);
		assertTrue(channel.isOpen());
		assertTrue(channel.isConnected());
	}

	public void testLargeInsertion()
	{
		long start, stop;
		int nitems = 10000;

		try
		{
			logger.info("Inserting " + nitems + " elements");
			ReadItems mythread = new ReadItems(0, nitems);
			mythread.start();

			start = System.currentTimeMillis();
			for (int i = 0; i < nitems; i++)
				channel.send(new Message(null, null, new String("Msg #" + i).getBytes()));

			mythread.join();

			stop = System.currentTimeMillis();
			logger.info("Took " + (stop - start) + " msecs");

			assertEquals(nitems, mythread.getNum_items());
			assertFalse(mythread.isAlive());
		}
		catch (Exception ex)
		{
			logger.error("Problem", ex);
			assertTrue(false);
		}
	}

	/** 
	 * Multiple threads call receive(timeout), one threads then adds an element and another one.
	 * Only 2 threads should actually terminate
	 * (the ones that had the element) 
	 */
	public void testBarrierWithTimeOut()
	{
		RemoveOneItemWithTimeout[] removers = new RemoveOneItemWithTimeout[10];
		int num_dead = 0;
		long timeout = 200;

		for (int i = 0; i < removers.length; i++)
		{
			removers[i] = new RemoveOneItemWithTimeout(i, timeout);
			removers[i].start();
		}

		Util.sleep(5000);

		logger.info("-- adding element 99");
		try
		{
			channel.send(null, null, new Long(99));
		}
		catch (Exception ex)
		{
			logger.error("Problem", ex);
		}

		Util.sleep(5000);
		logger.info("-- adding element 100");
		try
		{
			channel.send(null, null, new Long(100));
		}
		catch (Exception ex)
		{
			logger.error("Problem", ex);
		}

		Util.sleep(5000);

		for (int i = 0; i < removers.length; i++)
		{
			logger.info("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
			if (!removers[i].isAlive())
			{
				num_dead++;
			}
		}

		assertEquals(2, num_dead);

		// Testing handling of disconnect by reader threads
		channel.disconnect();
		// channel.close();
		Util.sleep(2000);

		for (int i = 0; i < removers.length; i++)
		{
			try
			{
				logger.debug("Waiting for thread " + i + " to join");
				removers[i].join();
			}
			catch (InterruptedException e)
			{
				logger.error("Thread joining() interrupted", e);
			}
		}

		num_dead = 0;
		for (int i = 0; i < removers.length; i++)
		{
			logger.info("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
			if (!removers[i].isAlive())
			{
				num_dead++;
			}
		}
		assertEquals(removers.length, num_dead);

		// help garbage collecting
		for (int i = 0; i < removers.length; i++)
		{
			removers[i] = null;
		}
	}

	/** 
	 * Multiple threads add one element, one thread read them all.
	 */
	public void testMultipleWriterOneReader()
	{
		AddOneItem[] adders = new AddOneItem[10];
		int num_dead = 0;
		int num_items = 0;
		int items = 200;

		for (int i = 0; i < adders.length; i++)
		{
			adders[i] = new AddOneItem(i, items);
			adders[i].start();
		}

		Object obj;
		Message msg;
		while (num_items < (adders.length * items))
		{

			try
			{
				obj = channel.receive(0); // no timeout
				if (obj instanceof View)
					logger.info("--> NEW VIEW: " + obj);
				else if (obj instanceof Message)
				{
					msg = (Message) obj;
					num_items++;
					logger.debug("Received " + msg.getObject());
				}
			}
			catch (ChannelNotConnectedException conn)
			{
				logger.error("Problem", conn);
				assertTrue(false);
				break;
			}
			catch (TimeoutException e)
			{
				logger.error("Main thread timed out but should'nt had...", e);
				assertTrue(false);
				break;
			}
			catch (Exception e)
			{
				logger.error("Problem", e);
				break;
			}

		}
		assertEquals(adders.length * items, num_items);

		Util.sleep(1000);

		for (int i = 0; i < adders.length; i++)
		{
			try
			{
				logger.debug("Waiting for thread " + i + " to join");
				adders[i].join();
				logger.info("adder #" + i + " is " + (adders[i].isAlive() ? "alive" : "terminated"));
				if (!adders[i].isAlive())
				{
					num_dead++;
				}
				adders[i] = null;
			}
			catch (InterruptedException e)
			{
				logger.error("Thread joining() interrupted", e);
			}
		}

		assertEquals(adders.length, num_dead);
	}

	/** 
	 * Multiple threads call receive(0), one threads then adds an element and another one.
	 * Only 2 threads should actually terminate
	 * (the ones that had the element) 
	 * NOTA: This test must be the last one as threads are not stopped at the end of the test.
	 */
	public void testBarrier()
	{
		ReadItems[] removers = new ReadItems[10];
		int num_dead = 0;

		for (int i = 0; i < removers.length; i++)
		{
			removers[i] = new ReadItems(i, 1);
			removers[i].start();
		}

		Util.sleep(1000);

		logger.info("-- adding element 99");
		try
		{
			channel.send(null, null, new String("99").getBytes());
		}
		catch (Exception ex)
		{
			logger.error("Problem", ex);
		}

		Util.sleep(5000);
		logger.info("-- adding element 100");
		try
		{
			channel.send(null, null, new String("100").getBytes());
		}
		catch (Exception ex)
		{
			logger.error("Problem", ex);
		}

		Util.sleep(1000);

		for (int i = 0; i < removers.length; i++)
		{
			logger.info("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
			if (!removers[i].isAlive())
			{
				num_dead++;
			}
		}

		assertEquals(2, num_dead);

	}

	/** 
	 * Multiple threads add one element, one thread read them all.
	 */
	public void testMultipleWriterMultipleReader()
	{
		logger.info("start testMultipleWriterMultipleReader");
		int nWriters = 3;
		int nReaders = 5;

		Writer[] adders = new Writer[nWriters];
		Reader[] readers = new Reader[nReaders];
		int num_dead = 0;
		int num_items = 0;
		int[] writes = new int[nWriters];
		int[] reads = new int[nReaders];

		for (int i = 0; i < readers.length; i++)
		{
			readers[i] = new Reader(channel, i, reads);
			readers[i].start();
		}

		Util.sleep(2000);

		for (int i = 0; i < adders.length; i++)
		{
			adders[i] = new Writer(channel, i, writes);
			adders[i].start();
		}

		// give writers time to write !
		Util.sleep(15000);

		// stop all writers
		for (int i = 0; i < adders.length; i++)
		{
			adders[i].stopThread();
		}

		// give time to writers to stop
		Util.sleep(1000);

		// checking all writers are stopped
		for (int i = 0; i < adders.length; i++)
		{
			try
			{
				logger.debug("Waiting for Writer thread " + i + " to join");
				adders[i].join(1000);
				logger.info("adder #" + i + " is " + (adders[i].isAlive() ? "alive" : "terminated"));
				adders[i] = null;
			}
			catch (InterruptedException e)
			{
				logger.error("Thread joining() interrupted", e);
			}
		}

		// give time for readers to read back data
		Util.sleep(10000);

		logger.info("Number of messages in channel queue :"+channel.getNumMessages());
		channel.close();

		// give time to readers to catch ChannelClosedException
		boolean allStopped = true;
		do
		{
			allStopped = true;
			Util.sleep(2000);
			for (int i = 0; i < readers.length; i++)
			{
				try
				{
					logger.debug("Waiting for Reader thread " + i + " to join");
					readers[i].join(1000);
					if (readers[i].isAlive())
					{
						allStopped = false;
						logger.info("reader #"+i+" "+reads[i]+" read items");
					}
					logger.info("reader #" + i + " is " + (readers[i].isAlive() ? "alive" : "terminated"));
				}
				catch (InterruptedException e)
				{
					logger.error("Thread joining() interrupted", e);
				}
			}
		}
		while (!allStopped);

		int total_writes = 0;
		for (int i = 0; i < writes.length; i++)
		{
			total_writes += writes[i];
		}

		int total_reads = 0;
		for (int i = 0; i < reads.length; i++)
		{
			total_reads += reads[i];
		}
		logger.info("Total writes:" + total_writes);
		logger.info("Total reads:" + total_reads);

		assertEquals(total_writes, total_reads);

		logger.info("end testMultipleWriterMultipleReader");
	}



	class ReadItems extends Thread
	{
		private boolean looping = true;
		int num_items = 0;
		int max = 0;
		int rank;

		public ReadItems(int rank, int num)
		{
			super("ReadItems thread #" + rank);
			this.rank = rank;
			this.max = num;
			setDaemon(true);
		}

		public void stopLooping()
		{
			looping = false;
		}

		public void run()
		{
			Object obj;
			Message msg;
			while (looping)
			{
				try
				{
					obj = channel.receive(0); // no timeout
					if (obj instanceof View)
						logger.info("Thread #" + rank + ":--> NEW VIEW: " + obj);
					else if (obj instanceof Message)
					{
						msg = (Message) obj;
						num_items++;
						if (num_items >= max)
							looping = false;
						logger.debug("Thread #" + rank + " received :" + new String(msg.getBuffer()));
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Thread #" + rank + ": problem", conn);
					looping = false;
				}
				catch (TimeoutException e)
				{
					logger.error("Thread #" + rank + ": channel timed out but should'nt have...", e);
					looping = false;
				}
				catch (ChannelClosedException e)
				{
					logger.debug("Thread #" + rank + ": channel closed", e);
					looping = false;
				}
				catch (Exception e)
				{
					logger.error("Thread #" + rank + ": problem", e);
					looping = false;
				}
			}
		}
		/**
		 * @return
		 */
		public int getNum_items()
		{
			return num_items;
		}

	}

	class RemoveOneItem extends Thread
	{
		private boolean looping = true;
		int rank;
		Long retval = null;

		public RemoveOneItem(int rank)
		{
			super("RemoveOneItem thread #" + rank);
			this.rank = rank;
			setDaemon(true);
		}

		public void stopLooping()
		{
			looping = false;
		}

		public void run()
		{
			Object obj;
			Message msg;
			while (looping)
			{
				try
				{
					obj = channel.receive(0); // no timeout
					if (obj instanceof View)
						logger.info("Thread #" + rank + ":--> NEW VIEW: " + obj);
					else if (obj instanceof Message)
					{
						msg = (Message) obj;
						looping = false;
						retval = (Long) msg.getObject();
						logger.debug("Thread #" + rank + ": received " + retval);
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Thread #" + rank + ": problem", conn);
					looping = false;
				}
				catch (TimeoutException e)
				{
					logger.error("Thread #" + rank + ": channel time out but should'nt have...", e);
					looping = false;
				}
				catch (Exception e)
				{
					logger.error("Thread #" + rank + ": problem", e);
				}
			}
		}
		Long getRetval()
		{
			return retval;
		}
	}

	class AddOneItem extends Thread
	{
		Long retval = null;
		int rank = 0;
		int iteration = 0;

		AddOneItem(int rank, int iteration)
		{
			super("AddOneItem thread #" + rank);
			this.rank = rank;
			this.iteration = iteration;
			setDaemon(true);
		}

		public void run()
		{
			try
			{
				for (int i = 0; i < iteration; i++)
				{
					channel.send(null, null, new Long(rank));
					logger.debug("Thread #" + rank + " added element (" + rank + ")");
				}
			}
			catch (ChannelException ex)
			{
				logger.error("Thread #" + rank + ": channel was closed", ex);
			}
		}

	}

	class RemoveOneItemWithTimeout extends Thread
	{
		Long retval = null;
		int rank = 0;
		long timeout = 0;

		RemoveOneItemWithTimeout(int rank, long timeout)
		{
			super("RemoveOneItemWithTimeout thread #" + rank);
			this.rank = rank;
			this.timeout = timeout;
			setDaemon(true);
		}

		public void run()
		{
			boolean finished = false;
			Object obj;
			Message msg;
			while (!finished)
			{
				try
				{
					obj = channel.receive(timeout);
					if (obj != null)
					{
						if (obj instanceof View)
							logger.info("Thread #" + rank + ":--> NEW VIEW: " + obj);
						else if (obj instanceof Message)
						{
							msg = (Message) obj;
							retval = (Long) msg.getObject();
							finished = true;
							logger.debug("Thread #" + rank + " received :" + retval);
						}
					}
					else
					{
						logger.debug("Thread #" + rank + ": channel read NULL");
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Thread #" + rank + " problem", conn);
					finished = true;
				}
				catch (TimeoutException e)
				{
					// logger.debug("Thread #" + rank + ": channel timed out", e);
				}
				catch (ChannelClosedException e)
				{
					logger.debug("Thread #" + rank + ": channel closed", e);
					finished = true;
				}
				catch (Exception e)
				{
					logger.error("Thread #" + rank + " problem", e);
					finished = true;
				}
			}
		}

		Long getRetval()
		{
			return retval;
		}
	}

	class Writer extends Thread
	{
		int rank = 0;
		int num_writes = 0;
		boolean running = true;
		int[] writes = null;
		Channel channel = null;

		Writer(Channel channel, int i, int[] writes)
		{
			super("WriterThread");
			rank = i;
			this.writes = writes;
			this.channel = channel;
			setDaemon(true);
		}

		public void run()
		{
			while (running)
			{
				try
				{
					channel.send(null, null, new Long(System.currentTimeMillis()));
					Util.sleepRandom(50);
					num_writes++;
				}
				catch (ChannelException closed)
				{
					running = false;
				}
				catch (Throwable t)
				{
					logger.debug("ChannelTest.Writer.run(): exception=" + t, t);
				}
			}
			writes[rank] = num_writes;
		}

		void stopThread()
		{
			running = false;
		}
	}

	class Reader extends Thread
	{
		int rank;
		int num_reads = 0;
		int[] reads = null;
		boolean running = true;
		Channel channel = null;

		Reader(Channel channel, int i, int[] reads)
		{
			super("Reader thread #" + i);
			rank = i;
			this.reads = reads;
			setDaemon(true);
			this.channel = channel;
		}

		public void run()
		{
			Long retval;
			Message msg = null;

			while (running)
			{
				try
				{
					Object obj = channel.receive(0); // no timeout
					if (obj instanceof View)
						logger.info("Reader thread #" + rank + ":--> NEW VIEW: " + obj);
					else if (obj instanceof Message)
					{
						msg = (Message) obj;
						retval = (Long) msg.getObject();
						logger.debug("Reader thread #" + rank + ": received " + retval);
						num_reads++;
						assertNotNull(retval);
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Reader thread #" + rank + ": problem", conn);
					running = false;
				}
				catch (TimeoutException e)
				{
					logger.error("Reader thread #" + rank + ": channel time out but should'nt have...", e);
					running = false;
				}
				catch (ChannelClosedException e)
				{
					logger.error("Reader thread #" + rank + ": channel closed", e);
					running = false;
				}
				catch (Exception e)
				{
					logger.error("Reader thread #" + rank + ": problem", e);
				}
			}
			reads[rank] = num_reads;
		}

		void stopThread()
		{
			running = false;
		}

	}

	public static void main(String[] args)
	{
		String[] testCaseName = { ChannelMonoTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}

}
