// $Id: ChannelMonoTest.java,v 1.1 2003/09/18 09:52:56 rds13 Exp $

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

			stop = System.currentTimeMillis();
			logger.info("Took " + (stop - start) + " msecs");

			Util.sleep(2000);
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
	 * Multiple threads call receive(0), one threads then adds an element and another one.
	 * Only 2 threads should actually terminate
	 * (the ones that had the element) 
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
	 * Multiple threads call receive(timeout), one threads then adds an element and another one.
	 * Only 2 threads should actually terminate
	 * (the ones that had the element) 
	 */
	public void testBarrierWithTimeOut()
	{
		RemoveOneItemWithTimeout[] removers = new RemoveOneItemWithTimeout[10];
		int num_dead = 0;
		long timeout = 1000;

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

		channel.disconnect();
		channel.close();

		Util.sleep(20000);

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
			logger.info("adder #" + i + " is " + (adders[i].isAlive() ? "alive" : "terminated"));
			if (!adders[i].isAlive())
			{
				num_dead++;
			}
		}

		assertEquals(adders.length, num_dead);

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
						logger.info("--> NEW VIEW: " + obj);
					else if (obj instanceof Message)
					{
						msg = (Message) obj;
						num_items++;
						if (num_items >= max)
							looping = false;
						logger.debug("Received " + new String(msg.getBuffer()));
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Problem", conn);
					looping = false;
				}
				catch (TimeoutException e)
				{
					logger.error("Thread rank "+rank+": channel timed out but should'nt have...", e);
					looping = false;
				}
				catch (ChannelClosedException e)
				{
					logger.debug("Thread rank "+rank+": channel closed", e);
					looping = false;
				}
				catch (Exception e)
				{
					logger.error("Problem", e);
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
						logger.info("--> NEW VIEW: " + obj);
					else if (obj instanceof Message)
					{
						msg = (Message) obj;
						looping = false;
						retval = (Long) msg.getObject();
						logger.debug("Received " + retval);
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Problem", conn);
					looping = false;
				}
				catch (TimeoutException e)
				{
					logger.error("Thread rank "+rank+": channel time out but should'nt have...", e);
					looping = false;
				}
				catch (Exception e)
				{
					logger.error("Problem", e);
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
					if (obj instanceof View)
						logger.info("--> NEW VIEW: " + obj);
					else if (obj instanceof Message)
					{
						msg = (Message) obj;
						retval = (Long) msg.getObject();
						finished = true;
						logger.debug("Received " + retval);
					}
				}
				catch (ChannelNotConnectedException conn)
				{
					logger.error("Problem", conn);
					finished = true;
				}
				catch (TimeoutException e)
				{
					logger.debug("Thread #" + rank + ": channel timed out", e);
				}
				catch (ChannelClosedException e)
				{
					logger.debug("Thread #" + rank + ": channel closed", e);
					finished = true;
				}
				catch (Exception e)
				{
					logger.error("Problem", e);
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

		Writer(int i, int[] writes)
		{
			super("WriterThread");
			rank = i;
			this.writes = writes;
			setDaemon(true);
		}

		public void run()
		{
			while (running)
			{
				try
				{
					channel.send(null, null, new Long(System.currentTimeMillis()));
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

		Reader(int i, int[] reads)
		{
			super("ReaderThread");
			rank = i;
			this.reads = reads;
			setDaemon(true);
		}

		public void run()
		{
			Long el;

			while (running)
			{
				try
				{
					el = (Long) channel.receive(0);
					if (el == null)
					{ // @remove
						logger.info("ChannelTest.Reader.run(): peek() returned null element. ");
					}
					assertNotNull(el);
					num_reads++;
				}
				catch (ChannelException ex)
				{
					running = false;
					logger.debug("Terminating ?", ex);
				}
				catch (Throwable t)
				{
					logger.error("ChannelTest.Reader.run(): exception=", t);
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
