// $Id: ChannelDuo.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $

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
 * Tests operations on two Channel instance with various threads reading/writing :
 * - one writer, n readers with or without timeout
 * - one reader, n writers
 * @author rds13
 */
public class ChannelDuo extends TestCase
{
    final static boolean DEBUG = false;
    private Channel channel1 = null;
    private Channel channel2 = null;

    static Logger logger = Logger.getLogger(ChannelDuo.class);
    String channelName = "ChannelLog4jTest";
    String protocol = null;

    public ChannelDuo(String Name_)
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
        try
        {
            channel1 = new JChannel(null);
            channel1.connect(channelName);
        }
        catch (ChannelException e)
        {
            logger.error("Channel init problem", e);
        }
    }

    public void tearDown()
    {
        if (channel1 != null)
        {
            channel1.close();
            channel1 = null;
        }
    }

    public void testLargeInsertion()
    {
        long start, stop;
        int nitems = 10000;
        logger.debug("Starting testLargeInsertion");

        try
        {
            logger.info("Inserting " + nitems + " elements");
            ReadItems mythread = new ReadItems(channel1, 0, nitems);
            mythread.start();

            channel2 = new JChannel(null);
            channel2.connect(channelName);

            start = System.currentTimeMillis();
            for (int i = 0; i < nitems; i++)
                channel2.send(new Message(null, null, new String("Msg #" + i).getBytes()));
            mythread.join();
            stop = System.currentTimeMillis();
            logger.info("Took " + (stop - start) + " msecs");

            assertEquals(nitems, mythread.getNum_items());
            assertFalse(mythread.isAlive());

            channel2.close();
            channel2 = null;
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
            assertTrue(false);
        }
        logger.debug("end testLargeInsertion");
    }

    /** 
     * Multiple threads call receive(timeout), one threads then adds an element and another one.
     * Only 2 threads should actually terminate
     * (the ones that had the element) 
     */
    public void testBarrierWithTimeOut()
    {
        logger.info("start testBarrierWithTimeOut");

        RemoveOneItemWithTimeout[] removers = new RemoveOneItemWithTimeout[10];
        int num_dead = 0;
        long timeout = 200;

        for (int i = 0; i < removers.length; i++)
        {
            removers[i] = new RemoveOneItemWithTimeout(channel1, i, timeout);
            removers[i].start();
        }

        try
        {
            channel2 = new JChannel(null);
            channel2.connect(channelName);
        }
        catch (Exception e)
        {
            logger.error("Problem", e);
        }

        Util.sleep(5000);

        logger.info("-- adding element 99");
        try
        {
            channel2.send(null, null, new Long(99));
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }

        Util.sleep(5000);
        logger.info("-- adding element 100");
        try
        {
            channel2.send(null, null, new Long(100));
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

        // Testing handling of disconnect by reader threads
        channel1.disconnect();
        // channel1.close();
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

        channel2.close();
        channel2 = null;

        logger.info("end testBarrierWithTimeOut");
    }

    /** 
     * Multiple threads add one element, one thread read them all.
     */
    public void testMultipleWriterOneReader()
    {
        logger.info("start testMultipleWriterOneReader");
        AddOneItem[] adders = new AddOneItem[10];
        int num_dead = 0;
        int num_items = 0;
        int items = 200;

        try
        {
            channel2 = new JChannel(null);
            channel2.connect(channelName);
        }
        catch (Exception e)
        {
            logger.error("Problem", e);
        }

        Util.sleep(1000);

        for (int i = 0; i < adders.length; i++)
        {
            adders[i] = new AddOneItem(channel1, i, items);
            adders[i].start();
        }

        Object obj;
        Message msg;
        while (num_items < (adders.length * items))
        {

            try
            {
                obj = channel2.receive(0); // no timeout
                if (obj instanceof View)
                    logger.info("--> NEW VIEW: " + obj);
                else if (obj instanceof Message)
                {
                    msg = (Message) obj;
                    num_items++;
                    logger.debug("Received " + msg.getObject());
                }
                else
                {
                    logger.error("Unexpected object type " + obj.getClass());
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
        channel2.close();
        channel2 = null;
        logger.info("end testMultipleWriterOneReader");
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

        ReadItems[] removers = new ReadItems[10];
        int num_dead = 0;

        for (int i = 0; i < removers.length; i++)
        {
            removers[i] = new ReadItems(channel1, i, 1);
            removers[i].start();
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

        logger.info("-- adding element 99");
        try
        {
            channel2.send(null, null, new String("99").getBytes());
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }

        Util.sleep(5000);
        logger.info("-- adding element 100");
        try
        {
            channel2.send(null, null, new String("100").getBytes());
        }
        catch (Exception ex)
        {
            logger.error("Problem", ex);
        }

        Util.sleep(2000);

        for (int i = 0; i < removers.length; i++)
        {
            logger.info("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
            if (!removers[i].isAlive())
            {
                num_dead++;
            }
        }

        assertEquals(2, num_dead);

        channel1.close();

        for (int i = 0; i < removers.length; i++)
        {
            try
            {
                removers[i].join(1000);
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

        channel2.close();
        channel2 = null;
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
        Reader[] readers = new Reader[nReaders];
        int num_dead = 0;
        int num_items = 0;
        int[] writes = new int[nWriters];
        int[] reads = new int[nReaders];

        try
        {
            channel2 = new JChannel(null);
            channel2.connect(channelName);
        }
        catch (Exception e)
        {
            logger.error("Problem", e);
        }

        for (int i = 0; i < readers.length; i++)
        {
            readers[i] = new Reader(channel2, i, reads);
            readers[i].start();
        }

        Util.sleep(2000);

        for (int i = 0; i < adders.length; i++)
        {
            adders[i] = new Writer(channel1, i, writes);
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
        Util.sleep(10000);

        channel2.close();

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
                        logger.info("reader #" + i + " " + reads[i] + " read items");
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

        channel2.close();
        channel2 = null;
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
        String[] testCaseName = { ChannelDuo.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
