// $Id: ChannelTrio.java,v 1.2 2004/03/30 06:47:31 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.jgroups.*;
import org.jgroups.util.Util;

/*
 * This time we will use 3 channels. Why ?
 * We want to check synchronization with two readers,
 * and one writer.
 * @author rds13
 */
public class ChannelTrio extends TestCase
{
    private Channel channel1 = null;
    private Channel channel2 = null;
    private Channel channel3 = null;

	final static boolean DEBUG = false;
    static Logger logger = Logger.getLogger(ChannelTrio.class);
    String channelName = "ChannelLog4jTest";
	String protocol = null;
	
    public ChannelTrio(String Name_)
    {
        super(Name_);
    }

	public String getProtocol()
	{
		return protocol;
	}
    
	public void setProtocol(String proto)
	{
		protocol = proto;
	}
	
    public void setUp()
    {
    }

    public void tearDown()
    {
    }

    public void testLargeInsertion()
    {
        long start, stop;
        int nitems = 10000;
        logger.info("start testLargeInsertion");

        try
        {
            logger.info("Inserting " + nitems + " elements");

            channel1 = new JChannel(null);
            channel1.connect(channelName);
            ReadItems rthread1 = new ReadItems(channel1, 0, nitems);
            rthread1.start();

            channel2 = new JChannel(null);
            channel2.connect(channelName);
            ReadItems rthread2 = new ReadItems(channel2, 0, nitems);
            rthread2.start();

            channel3 = new JChannel(null);
            channel3.connect(channelName);
            start = System.currentTimeMillis();
            for (int i = 0; i < nitems; i++)
                channel3.send(new Message(null, null, new String("Msg #" + i).getBytes()));

            rthread1.join();
            rthread2.join();

            stop = System.currentTimeMillis();
            logger.info("Took " + (stop - start) + " msecs");

			assertEquals(nitems, rthread1.getNum_items());
			assertEquals(nitems, rthread2.getNum_items());
            assertFalse(rthread1.isAlive());
            assertFalse(rthread2.isAlive());

            channel1.close();
            channel1 = null;
            channel2.close();
            channel2 = null;
            channel3.close();
            channel3 = null;
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
            assertTrue(false);
        }
        logger.info("end testLargeInsertion");
    }

    /** 
     * Multiple threads call receive(timeout), one threads then adds an element and another one.
     * Only 2 threads should actually terminate
     * (the ones that had the element) 
     */
    public void testBarrierWithTimeOut()
    {
        logger.info("start testBarrierWithTimeOut");

        RemoveOneItemWithTimeout[] removersGroupOne = new RemoveOneItemWithTimeout[10];
        RemoveOneItemWithTimeout[] removersGroupTwo = new RemoveOneItemWithTimeout[10];
        int num_dead = 0;
        long timeout = 200;

        try
        {

            channel1 = new JChannel(null);
            channel1.connect(channelName);
            for (int i = 0; i < removersGroupOne.length; i++)
            {
                removersGroupOne[i] = new RemoveOneItemWithTimeout(channel1, i, timeout);
                removersGroupOne[i].start();
            }

            channel2 = new JChannel(null);
            channel2.connect(channelName);
            for (int i = 0; i < removersGroupTwo.length; i++)
            {
                removersGroupTwo[i] = new RemoveOneItemWithTimeout(channel2, i, timeout);
                removersGroupTwo[i].start();
            }

            channel3 = new JChannel(null);
            channel3.connect(channelName);
        }
        catch (Exception e)
        {
            logger.error("Problem", e);
        }

        Util.sleep(5000);

        logger.info("-- adding element 99");
        try
        {
            channel3.send(null, null, new Long(99));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }

        Util.sleep(5000);
        logger.info("-- adding element 100");
        try
        {
            channel3.send(null, null, new Long(100));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }

        Util.sleep(1000);

        num_dead = 0;
        for (int i = 0; i < removersGroupOne.length; i++)
        {
            logger.info("removersGroupOne #" + i + " is " + (removersGroupOne[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupOne[i].isAlive())
            {
                num_dead++;
            }
        }
        int num_deadTwo = 0;
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            logger.info("removersGroupTwo #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupTwo[i].isAlive())
            {
                num_deadTwo++;
            }
        }

        assertEquals(2, num_dead);
		assertEquals(2, num_deadTwo);

        // Testing handling of disconnect by reader threads
        channel1.disconnect();
        // channel1.close();
        Util.sleep(2000);

        for (int i = 0; i < removersGroupOne.length; i++)
        {
            try
            {
                logger.debug("Waiting for remover Group One # " + i + " to join");
                removersGroupOne[i].join();
            }
            catch (InterruptedException e)
            {
                logger.error("Thread joining() interrupted", e);
            }
        }

        num_dead = 0;
        for (int i = 0; i < removersGroupOne.length; i++)
        {
            logger.info("remover Group One #" + i + " is " + (removersGroupOne[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupOne[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(removersGroupOne.length, num_dead);

		Util.sleep(2000);
		num_dead = 0;
		logger.info("though Group One stopped, Group Two shall continue");
		// though Group One stopped, Group Two shall continue
		for (int i = 0; i < removersGroupTwo.length; i++)
		{
			logger.info("removersGroupTwo #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
			if (!removersGroupTwo[i].isAlive())
			{
				num_dead++;
			}
		}
		assertEquals("Readers thread from Group Two stop that should'nt", 2, num_dead);

        logger.info("-- adding element 101");
        try
        {
            channel3.send(null, null, new Long(101));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
        Util.sleep(5000);
        logger.info("-- adding element 102");
        try
        {
            channel3.send(null, null, new Long(102));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
		Util.sleep(5000);

        num_dead = 0;
		logger.info("Checking only 4 Group Two threads should have stop");
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            logger.info("removersGroupTwo #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupTwo[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(2, num_dead - num_deadTwo);

        channel2.close();
        channel2 = null;

        Util.sleep(2000);

        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            try
            {
                logger.debug("Waiting for removersGroupTwo " + i + " to join");
                removersGroupTwo[i].join();
            }
            catch (InterruptedException e)
            {
                logger.error("Thread joining() interrupted", e);
            }
        }

        num_dead = 0;
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            logger.info("removersGroupTwo #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupTwo[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(removersGroupTwo.length, num_dead);

        channel3.close();

        logger.info("end testBarrierWithTimeOut");
    }

    /** 
     * Multiple threads call receive(0), one threads then adds an element and another one.
     * Only 2 threads should actually terminate
     * (the ones that had the element) 
     * NOTA: This test must be the last one as threads are not stopped at the end of the test.
     */
    public void testBarrier()
    {
        logger.info("start testBarrier");

        ReadItems[] removersGroupOne = new ReadItems[10];
        ReadItems[] removersGroupTwo = new ReadItems[10];
        int num_dead = 0;

        try
        {
            channel1 = new JChannel(null);
            channel1.connect(channelName);
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
        for (int i = 0; i < removersGroupOne.length; i++)
        {
            removersGroupOne[i] = new ReadItems(channel1, i, 1);
            removersGroupOne[i].start();
        }

        try
        {
            channel2 = new JChannel(null);
            channel2.connect(channelName);
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
        Util.sleep(1000);

        logger.info("-- adding Msg #1");
        try
        {
            channel2.send(new Message(null, null, new String("Msg #1").getBytes()));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
		Util.sleep(1000);

        try
        {
            channel3 = new JChannel(null);
            channel3.connect(channelName);
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
        Util.sleep(5000);
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            removersGroupTwo[i] = new ReadItems(channel3, i, 1);
            removersGroupTwo[i].start();
        }

        logger.info("-- adding Msg #2");
        try
        {
            channel2.send(new Message(null, null, new String("Msg #2").getBytes()));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }

        Util.sleep(2000);

        num_dead = 0;
        for (int i = 0; i < removersGroupOne.length; i++)
        {
            logger.info("removersGroupOne #" + i + " is " + (removersGroupOne[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupOne[i].isAlive())
            {
                num_dead++;
            }
        }
        int num_deadTwo = 0;
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            logger.info("removersGroupTwo #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupTwo[i].isAlive())
            {
                num_deadTwo++;
            }
        }

		assertEquals(2, num_dead);
		assertEquals(1, num_deadTwo);

        try
        {
            logger.info("-- adding Msg #3");
            channel2.send(new Message(null, null, new String("Msg #3").getBytes()));
            logger.info("-- adding Msg #4");
            channel2.send(new Message(null, null, new String("Msg #4").getBytes()));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }
        Util.sleep(2000);

		num_dead = 0;
		for (int i = 0; i < removersGroupOne.length; i++)
		{
			logger.info("removersGroupOne #" + i + " is " + (removersGroupOne[i].isAlive() ? "alive" : "terminated"));
			if (!removersGroupOne[i].isAlive())
			{
				num_dead++;
			}
		}
		assertEquals(4, num_dead);

        channel1.close();
        for (int i = 0; i < removersGroupOne.length; i++)
        {
            try
            {
                removersGroupOne[i].join(1000);
            }
            catch (InterruptedException e)
            {
                logger.error("Thread joining() interrupted", e);
            }
        }

        num_dead = 0;
        for (int i = 0; i < removersGroupOne.length; i++)
        {
            logger.info("remover #" + i + " is " + (removersGroupOne[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupOne[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(removersGroupOne.length, num_dead);

        num_dead = 0;
        // check no more threads dead in group two
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            logger.info("remover #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupTwo[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(num_deadTwo+2, num_dead);

        channel2.close();
        channel2 = null;
        channel3.close();
        channel3 = null;

        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            try
            {
                removersGroupTwo[i].join(1000);
            }
            catch (InterruptedException e)
            {
                logger.error("Thread joining() interrupted", e);
            }
        }

        num_dead = 0;
        // check no more threads dead in group two
        for (int i = 0; i < removersGroupTwo.length; i++)
        {
            logger.info("remover Group Two #" + i + " is " + (removersGroupTwo[i].isAlive() ? "alive" : "terminated"));
            if (!removersGroupTwo[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(removersGroupTwo.length, num_dead);

        logger.info("stop testBarrier");
    }

    /** 
     * Multiple threads add one element, one thread read them all.
     */
    public void testMultipleWriterMultipleReader()
    {
        logger.info("start testMultipleWriterMultipleReader");
        int nWriters = 10;
        int nReaders = 10;

        Writer[] adders = new Writer[nWriters];
        Reader[] readersOne = new Reader[nReaders];
        Reader[] readersTwo = new Reader[nReaders];
        int num_dead = 0;
        int num_items = 0;
        int[] writes = new int[nWriters];
        int[] readsOne = new int[nReaders];
        int[] readsTwo = new int[nReaders];

        try
        {
            channel2 = new JChannel(null);
            channel2.connect(channelName);
            channel1 = new JChannel(null);
            channel1.connect(channelName);
        }
        catch (Exception e)
        {
            logger.error("Problem", e);
        }

        for (int i = 0; i < readersOne.length; i++)
        {
            readersOne[i] = new Reader(channel1, i, readsOne);
            readersOne[i].start();
        }
        for (int i = 0; i < readersTwo.length; i++)
        {
            readersTwo[i] = new Reader(channel2, i, readsTwo);
            readersTwo[i].start();
        }

        Util.sleep(2000);

        try
        {
            channel3 = new JChannel(null);
            channel3.connect(channelName);
        }
        catch (Exception e)
        {
            logger.error("Problem", e);
        }
        for (int i = 0; i < adders.length; i++)
        {
            adders[i] = new Writer(channel3, i, writes);
            adders[i].start();
        }

        // give writers time to write !
        Util.sleep(10000);

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
        Util.sleep(5000);

        channel2.close();
        channel1.close();

        // give time to readers to catch ChannelClosedException
        boolean allStopped = true;
        do
        {
            allStopped = true;
            Util.sleep(2000);
            for (int i = 0; i < readersOne.length; i++)
            {
                try
                {
                    logger.debug("Waiting for ReaderGroupOne thread " + i + " to join");
                    readersOne[i].join(1000);
                    if (readersOne[i].isAlive())
                    {
                        allStopped = false;
                        logger.info("reader One #" + i + " " + readsOne[i] + " read items");
                    }
                    logger.info("reader One #" + i + " is " + (readersOne[i].isAlive() ? "alive" : "terminated"));
                }
                catch (InterruptedException e)
                {
                    logger.error("Thread joining() interrupted", e);
                }
            }
        }
        while (!allStopped);

        allStopped = true;
        do
        {
            allStopped = true;
            Util.sleep(2000);
            for (int i = 0; i < readersTwo.length; i++)
            {
                try
                {
                    logger.debug("Waiting for ReaderGroupTwo thread " + i + " to join");
                    readersTwo[i].join(1000);
                    if (readersTwo[i].isAlive())
                    {
                        allStopped = false;
                        logger.info("reader Two #" + i + " " + readsTwo[i] + " read items");
                    }
                    logger.info("reader Two #" + i + " is " + (readersTwo[i].isAlive() ? "alive" : "terminated"));
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
        for (int i = 0; i < readsOne.length; i++)
        {
            total_reads += readsOne[i];
        }
        for (int i = 0; i < readsTwo.length; i++)
        {
            total_reads += readsTwo[i];
        }
        logger.info("Total writes:" + total_writes);
        logger.info("Total reads:" + total_reads);

        assertEquals(2*total_writes, total_reads);

        channel1.close();
        channel2.close();
        channel3.close();
        logger.info("end testMultipleWriterMultipleReader");
    }

    class ReadItems extends Thread
    {
        private boolean looping = true;
        int num_items = 0;
        int max = 0;
        int rank;
        Channel channel;

        public ReadItems(Channel channel, int rank, int num)
        {
            super("ReadItems thread #" + rank);
            this.rank = rank;
            this.max = num;
            this.channel = channel;
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
                    obj = channel1.receive(0); // no timeout
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
        Channel channel = null;

        AddOneItem(Channel channel, int rank, int iteration)
        {
            super("AddOneItem thread #" + rank);
            this.rank = rank;
            this.iteration = iteration;
            setDaemon(true);
            this.channel = channel;
        }

        public void run()
        {
            try
            {
                for (int i = 0; i < iteration; i++)
                {
                    channel.send(null, null, new Long(rank));
                    logger.debug("Thread #" + rank + " added element (" + rank + ")");
                    Util.sleepRandom(100);
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
        Channel channel = null;

        RemoveOneItemWithTimeout(Channel channel, int rank, long timeout)
        {
            super("RemoveOneItemWithTimeout thread #" + rank);
            this.rank = rank;
            this.timeout = timeout;
            setDaemon(true);
            this.channel = channel;
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
                	if (DEBUG)
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
            super("Writer thread #" + i);
            rank = i;
            this.writes = writes;
            setDaemon(true);
            this.channel = channel;
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
                	if (DEBUG)
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
        String[] testCaseName = { ChannelTrio.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
