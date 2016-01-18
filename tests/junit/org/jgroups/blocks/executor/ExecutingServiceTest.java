package org.jgroups.blocks.executor;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.executor.ExecutionService.DistributedFuture;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.CENTRAL_EXECUTOR;
import org.jgroups.protocols.AbstractExecuting.Owner;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.NotSerializableException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/** Tests {@link org.jgroups.blocks.executor.ExecutionService}
 * @author wburns
 */
@Test(groups={Global.STACK_DEPENDENT,Global.EAP_EXCLUDED},singleThreaded=true)
public class ExecutingServiceTest extends ChannelTestBase {
    protected static Log logger=null;
    protected ExposedExecutingProtocol exposedProtocol;
    
    protected JChannel         a, b, c;
    protected ExecutionService e1, e2, e3;
    protected ExecutionRunner  er1, er2, er3;
    
    @BeforeClass
    protected void init() throws Exception {
        logger = LogFactory.getLog(ExecutingServiceTest.class);
        a=createChannel(true, 3, "A");
        
        // Add the exposed executing protocol
        ProtocolStack stack=a.getProtocolStack();
        exposedProtocol = new ExposedExecutingProtocol();
        exposedProtocol.setLevel("debug");
        stack.insertProtocolAtTop(exposedProtocol);
        
        er1=new ExecutionRunner(a);
        a.connect("ExecutionServiceTest");

        b=createChannel(a, "B");
        er2=new ExecutionRunner(b);
        b.connect("ExecutionServiceTest");

        c=createChannel(a, "C");
        er3=new ExecutionRunner(c);
        c.connect("ExecutionServiceTest");

        Util.waitUntilAllChannelsHaveSameSize(10000, 1000,a,b,c);
        
        LogFactory.getLog(ExecutionRunner.class).setLevel("debug");
    }
    
    @AfterClass
    protected void cleanup() {
        Util.close(c,b,a);
    }
    
    @BeforeMethod
    protected void createExecutors() {
        e1=new ExecutionService(a);
        e2=new ExecutionService(b);
        e3=new ExecutionService(c);
        
        // Clear out the queue, in case if test doesn't clear it
        SleepingStreamableCallable.canceledThreads.clear();
        // Reset the barrier in case test failed on the barrier
        SleepingStreamableCallable.barrier.reset();
    }
    
    @AfterMethod
    protected void resetBlockers() {
        CyclicBarrier barrier = ExposedExecutingProtocol.requestBlocker.getAndSet(null);
        if (barrier != null) barrier.reset();
    }
    
    @AfterMethod
    protected void makeSureClean() {
        assert e1._unfinishedFutures.isEmpty() : "Unfinished e1 futures should be empty!";
        assert e2._unfinishedFutures.isEmpty() : "Unfinished e2 futures should be empty!";
    }
    
    public static class ExposedExecutingProtocol extends CENTRAL_EXECUTOR {
        public ExposedExecutingProtocol() {
            // We use the same id as the CENTRAL_EXECUTOR
            id=ClassConfigurator.getProtocolId(CENTRAL_EXECUTOR.class);
        }
        
        // @see org.jgroups.protocols.Executing#sendRequest(org.jgroups.Address, org.jgroups.protocols.Executing.Type, long, java.lang.Object)
        @Override
        protected void sendRequest(Address dest, Type type, long requestId,
                                   Object object) {
            CyclicBarrier barrier = requestBlocker.get();
            if (barrier != null) {
                try {
                    // The first wait is to make sure the caller knows
                    // they can now close the channel
                    barrier.await();
                    // The second wait is for us to be notified that the
                    // channel is now down
                    barrier.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    assert false : Thread.currentThread().getId() + 
                        "Exception while waiting: " + e.toString();
                }
                catch (BrokenBarrierException e) {
                    e.printStackTrace();
                    assert false : Thread.currentThread().getId() + 
                        "Exception while waiting: " + e.toString();
                }
            }
            super.sendRequest(dest, type, requestId, object);
        }
        
        public Queue<Runnable> getAwaitingConsumerQueue() {
            return _awaitingConsumer;
        }
        
        public Queue<Owner> getRequestsFromCoordinator() {
            return _runRequests;
        }
        
        public Lock getLock() {
            return _consumerLock;
        }
        
        public AtomicLong getCounter() {
            return counter;
        }
        
        public static final AtomicReference<CyclicBarrier> requestBlocker = 
            new AtomicReference<>();
    }

    /**
     * This class is to be used to test to make sure that when a non callable
     * is to be serialized that it works correctly.
     * <p>
     * This class provides a few constructors that shouldn't be used.  They
     * are just present to possibly poke holes in the constructor array offset.
     * @param <V> The type that the value can be returned as
     * @author wburns
     */
    protected static class SimpleCallable<V> implements Callable<V> {
        final V _object;
        
        // This constructor shouldn't be used
        public SimpleCallable(String noUse) {
            throw new UnsupportedOperationException();
        }
        
        // This constructor shouldn't be used
        public SimpleCallable(Integer noUse) {
            throw new UnsupportedOperationException();
        }
        
        public SimpleCallable(V object) {
            _object = object;
        }
        
        // This constructor shouldn't be used
        public SimpleCallable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V call() throws Exception {
            return _object;
        }
    }
    
    protected static class SleepingStreamableCallable implements Callable<Void>, Streamable {
        long millis;
        
        // These have to be static, since they cannot be serialized and the
        // test calls back to the same jvm so they will hit the same barrier
        public static BlockingQueue<Thread> canceledThreads = new LinkedBlockingQueue<>();
        public static CyclicBarrier barrier = new CyclicBarrier(2);
        
        public SleepingStreamableCallable() {
            
        }
        
        public SleepingStreamableCallable(long millis) {
            this.millis=millis;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(millis);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            millis = in.readLong();
        }

        @Override
        public Void call() throws Exception {
            barrier.await();
            try {
                Thread.sleep(millis);
            }
            catch (InterruptedException e) {
                Thread interruptedThread = Thread.currentThread();
                if (logger.isTraceEnabled())
                    logger.trace("Submitted cancelled thread - " + interruptedThread);
                canceledThreads.offer(interruptedThread);
            }
            return null;
        }
        
        // @see java.lang.Object#toString()
        @Override
        public String toString() {
            return "SleepingStreamableCallable [timeout=" + millis + "]";
        }
    }
    
    protected static class SimpleStreamableCallable<V> implements Callable<V>, Streamable {
        V _object;
        
        public SimpleStreamableCallable() {
            
        }

        public SimpleStreamableCallable(V object) {
            _object = object;
        }

        @Override
        public V call() throws Exception {
            return _object;
        }

        // @see java.lang.Object#toString()
        @Override
        public String toString() {
            return "SimpleSerializableCallable [value=" + _object + "]";
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            try {
                Util.writeObject(_object, out);
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readFrom(DataInput in) throws Exception {
            try {
                _object = (V)Util.readObject(in);
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
    
    @Test
    public void testSimpleSerializableCallableSubmit() 
            throws InterruptedException, ExecutionException, TimeoutException {
        long value =100;
        Callable<Long> callable = new SimpleStreamableCallable<>(value);
        Thread consumer = new Thread(er2);
        consumer.start();
        NotifyingFuture<Long> future = e1.submit(callable);
        Long returnValue = future.get(10L, TimeUnit.SECONDS);
        // We try to stop the thread.
        consumer.interrupt();
        assert value == returnValue : "The value returned doesn't match";
        
        consumer.join(2000);
        assert !consumer.isAlive() : "Consumer did not stop correctly";
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testSimpleSerializableCallableConcurrently() 
            throws InterruptedException, ExecutionException, TimeoutException {
        Thread[] consumers = {new Thread(er1), new Thread(er2), new Thread(er3)};
        
        for (Thread thread : consumers) {
            thread.start();
        }
        
        Random random = new Random();
        
        int count = 100;
        Future[] futures1 = new Future[count];
        Future[] futures2 = new Future[count];
        Future[] futures3 = new Future[count];
        StringBuilder builder = new StringBuilder("base");
        for (int i = 0; i < count; i++) {
            builder.append(random.nextInt(10));
            String value = builder.toString();
            futures1[i] = e1.submit(new SimpleStreamableCallable(value));
            futures2[i] = e2.submit(new SimpleStreamableCallable(value));
            futures3[i] = e3.submit(new SimpleStreamableCallable(value));
        }
        
        for (int i = 0; i < count; i++) {
            // All 3 of the futures should have returned the same value
            Object value = futures1[i].get(10L, TimeUnit.SECONDS);
            assert value.equals(futures2[i].get(10L, TimeUnit.SECONDS));
            assert value.equals(futures3[i].get(10L, TimeUnit.SECONDS));
            
            // Make sure that same value is what it should be
            CharSequence seq = builder.subSequence(0, 5+i);
            assert value.equals(seq);
        }
        
        for (Thread consumer : consumers) {
            // We try to stop the thread.
            consumer.interrupt();
            
            consumer.join(2000);
            assert !consumer.isAlive() : "Consumer did not stop correctly";
        }
    }
    
    /**
     * Interrupts can have a lot of timing issues, so we run it a lot to make
     * sure we find all the issues.
     * @throws InterruptedException
     * @throws BrokenBarrierException
     * @throws TimeoutException
     */
    @Test
    public void testInterruptWhileRunningAlot() throws InterruptedException, BrokenBarrierException, TimeoutException {
        for (int i = 0; i < 500; ++i)
            testInterruptTaskRequestWhileRunning();
    }
    
    protected void testInterruptTaskRequestWhileRunning() 
            throws InterruptedException, BrokenBarrierException, TimeoutException {
        Callable<Void> callable = new SleepingStreamableCallable(10000);
        Thread consumer = new Thread(er2);
        consumer.start();
        NotifyingFuture<Void> future = e1.submit(callable);
        
        // We wait until it is ready
        SleepingStreamableCallable.barrier.await(5, TimeUnit.SECONDS);
        if (logger.isTraceEnabled())
            logger.trace("Cancelling future by interrupting");
        future.cancel(true);
        
        Thread cancelled = SleepingStreamableCallable.canceledThreads.poll(2, 
            TimeUnit.SECONDS);

        if (logger.isTraceEnabled())
            logger.trace("Cancelling task by interrupting");
        // We try to stop the thread now which should now stop the runner
        consumer.interrupt();
        assert cancelled != null : "There was no cancelled thread";
        
        consumer.join(2000);
        assert !consumer.isAlive() : "Consumer did not stop correctly";
    }
    
    @Test
    public void testInterruptTaskRequestBeforeRunning() 
            throws InterruptedException, TimeoutException {
        Callable<Void> callable = new SleepingStreamableCallable(10000);
        NotifyingFuture<Void> future = e1.submit(callable);
        
        // Now we make sure that callable is waiting for a consumer
        ExposedExecutingProtocol protocol = 
            (ExposedExecutingProtocol)a.getProtocolStack().findProtocol(
                ExposedExecutingProtocol.class);
        Queue<Runnable> queue = protocol.getAwaitingConsumerQueue();
        Lock lock = protocol.getLock();
        
        lock.lock();
        try {
            assert queue.peek() != null : "The object in queue doesn't match";
        }
        finally {
            lock.unlock();
        }
        // This should remove the task before it starts, since the consumer is
        // not yet running
        future.cancel(false);
        
        lock.lock();
        try {
            assert queue.peek() == null : "There should be no more objects in the queue";
        }
        finally {
            lock.unlock();
        }
    }
    
    @Test
    public void testExecutorAwaitTerminationNoInterrupt() throws InterruptedException, 
            BrokenBarrierException, TimeoutException {
        testExecutorAwaitTermination(false);
    }
    
    @Test
    public void testExecutorAwaitTerminationInterrupt() throws InterruptedException, 
            BrokenBarrierException, TimeoutException {
        testExecutorAwaitTermination(true);
    }
    
    protected void testExecutorAwaitTermination(boolean interrupt) 
            throws InterruptedException, BrokenBarrierException, TimeoutException {
        Thread consumer = new Thread(er2);
        consumer.start();
        // We send a task that waits for 101 milliseconds and then finishes
        Callable<Void> callable = new SleepingStreamableCallable(101);
        e1.submit(callable);
        
        // We wait for the thread to start
        SleepingStreamableCallable.barrier.await(10, TimeUnit.SECONDS);
        
        if (interrupt) {
            if (logger.isTraceEnabled())
                logger.trace("Cancelling futures by interrupting");
            e1.shutdownNow();
            // We wait for the task to be interrupted.
            assert SleepingStreamableCallable.canceledThreads.poll(2, 
                TimeUnit.SECONDS) != null : 
                    "Thread wasn't interrupted due to our request";
        }
        else {
            e1.shutdown();
        }
        
        assert e1.awaitTermination(2, TimeUnit.SECONDS) : "Executor didn't terminate fast enough";
        
        try {
            e1.submit(callable);
            assert false : "Task was submitted, where as it should have been rejected";
        }
        catch (RejectedExecutionException e) {
            // We should have received this exception
        }
        
        if (logger.isTraceEnabled())
            logger.trace("Cancelling task by interrupting");
        // We try to stop the thread.
        consumer.interrupt();
        
        consumer.join(2000);
        assert !consumer.isAlive() : "Consumer did not stop correctly";
    }
    
    @Test
    public void testNonSerializableCallable() throws SecurityException, 
            NoSuchMethodException, InterruptedException, ExecutionException, 
            TimeoutException {
        Thread consumer = new Thread(er2);
        consumer.start();
        
        long value =100;
        
        @SuppressWarnings("rawtypes")
        Constructor<SimpleCallable> constructor = 
            SimpleCallable.class.getConstructor(Object.class);
        @SuppressWarnings("unchecked")
        Callable<Long> callable = (Callable<Long>)Executions.serializableCallable(
            constructor, value);
        
        NotifyingFuture<Long> future = e1.submit(callable);
        Long returnValue = future.get(10L, TimeUnit.SECONDS);
        // We try to stop the thread.
        consumer.interrupt();
        assert value == returnValue : "The value returned doesn't match";
        
        consumer.join(2000);
        assert !consumer.isAlive() : "Consumer did not stop correctly";
    }
    
    @Test
    public void testExecutionCompletionService() throws InterruptedException {
        Thread consumer1 = new Thread(er2);
        consumer1.start();
        Thread consumer2 = new Thread(er3);
        consumer2.start();
        
        ExecutionCompletionService<Void> service = new ExecutionCompletionService<>(e1);
        
        // The sleeps will not occur until both threads get there due to barrier
        // This should result in future2 always ending first since the sleep
        // is 3 times smaller
        Future<Void> future1 = service.submit(new SleepingStreamableCallable(300));
        Future<Void> future2 = service.submit(new SleepingStreamableCallable(100));
        
        assert service.poll(2, TimeUnit.SECONDS) == future2 : "The task either didn't come back or was in wrong order";
        assert service.poll(2, TimeUnit.SECONDS) == future1 : "The task either didn't come back or was in wrong order";
        
        // We try to stop the threads.
        consumer1.interrupt();
        consumer2.interrupt();
        
        consumer1.join(2000);
        assert !consumer1.isAlive() : "Consumer did not stop correctly";
        consumer2.join(2000);
        assert !consumer2.isAlive() : "Consumer did not stop correctly";
    }
    
    @Test
    public void testCoordinatorWentDownWhileSendingMessage() throws Exception {
        // We sleep for 1 second to make sure other tests are done with messages
        // since the barrier being inserted can be picked up by other threads
        Thread.sleep(1000);
        // It is 3 calls.
        // The first is the original message sending to the coordinator
        // The second is the new message to send the request to the new coordinator
        // The last is our main method below waiting for others
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        ExposedExecutingProtocol.requestBlocker.set(barrier);
        
        int value = 23;
        final Callable<Integer> callable = new SimpleStreamableCallable<>(value);
        ExecutorService service = Executors.newCachedThreadPool();
        Future<Integer> future = service.submit(new Callable<Integer>() {

            @Override
            public Integer call() throws InterruptedException, ExecutionException {
                return e2.submit(callable).get();
            }
        });
        
        // Wait for the message to be almost sent to coordinator
        barrier.await(2, TimeUnit.SECONDS);
        
        // Set the blocker to be null now that we know only our message is blocked
        // so we don't block anyone else accidentally
        ExposedExecutingProtocol.requestBlocker.set(null);
        
        // Take down the coordinator which forces c2 or B to take over
        Util.close(a);
        
        // Let the message go and see if it wasn't lost
        barrier.await(1, TimeUnit.SECONDS);
        
        // Reset the barrier to make sure no one messed up
        barrier.reset();
        
        // We need to reconnect the channel now
        a=createChannel(b, "A");
        er1=new ExecutionRunner(a);
        a.connect("ExecutionServiceTest");
        
        service.shutdown();
        service.awaitTermination(2, TimeUnit.SECONDS);
        
        // Now we make sure that the new coordinator has the requests
        ExposedExecutingProtocol protocol = 
            (ExposedExecutingProtocol)b.getProtocolStack().findProtocol(
                ExposedExecutingProtocol.class);
        Queue<Runnable> runnables = protocol.getAwaitingConsumerQueue();
        
        assert runnables.size() == 1 : "There is no runnable in the queue";
        Runnable task = runnables.iterator().next();
        assert task instanceof DistributedFuture : "The task wasn't a distributed future like we thought";
        assert callable == ((DistributedFuture<?>)task).getCallable() : "The inner callable wasn't the same";
        
        Queue<Owner> requests = protocol.getRequestsFromCoordinator();
        assert requests.size() == 1 : "There is no request in the coordinator queue - " + requests.size();
        Owner owner = requests.iterator().next();
        assert owner.getAddress().equals(b.getAddress()) : "The request Address doesn't match";
        // Counter is always one higher than previously dished out id
        long expected = protocol.getCounter().get() -1;
        assert owner.getRequestId() == expected : "Request id " + 
            owner.getRequestId() + " didn't match what we expected " + expected; 
        
        // Start a consumer to consume it
        Thread consumer1 = new Thread(er2);
        consumer1.start();
        
        Integer returnValue = future.get(2, TimeUnit.SECONDS);
        assert returnValue == value : "Future value returne didn't match what we expected " + value + " was " + returnValue;
        
        // We try to stop the consumer
        consumer1.interrupt();
        
        consumer1.join(2000);
        assert !consumer1.isAlive() : "Consumer did not stop correctly";
    }
    
    @Test
    public void testInvokeAnyCalls() throws InterruptedException, ExecutionException {
        Thread consumer1 = new Thread(er2);
        consumer1.start();
        Thread consumer2 = new Thread(er3);
        consumer2.start();
        
        Collection<Callable<Long>> callables = new ArrayList<>();
        
        callables.add(new SimpleStreamableCallable<>((long)10));
        callables.add(new SimpleStreamableCallable<>((long)100));
        Long value = e1.invokeAny(callables);
        
        assert value == 10 || value == 100 : "The task didn't return the right value";
        
        // We try to stop the threads.
        consumer1.interrupt();
        consumer2.interrupt();
        
        consumer1.join(2000);
        assert !consumer1.isAlive() : "Consumer did not stop correctly";
        consumer2.join(2000);
        assert !consumer2.isAlive() : "Consumer did not stop correctly";
    }
    
    @Test
    public void testSubmittingNonSerializeCallable() {
        try {
            e1.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    return null;
                }
            });
            assert false : "Task was submitted, where as it should have thrown an exception";
        }
        catch (IllegalArgumentException e) {
            
        }
    }
    
    @Test
    public void testSubmittingSerializeCallableWithNonSerializableComponent() 
            throws InterruptedException, TimeoutException {
        Thread consumer1 = new Thread(er2);
        consumer1.start();
        
        Future<Object> future = e1.submit(new SimpleStreamableCallable<>(new Object()));
        try {
            future.get(10, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            Throwable t = e.getCause();
            assert t instanceof IllegalArgumentException : "Expected an illegal argument exception";
            
            Throwable t2 = t.getCause();
            assert t2 instanceof NotSerializableException : "Expected a not serializable exception";
        }
        
        // We try to stop the threads.
        consumer1.interrupt();
        
        consumer1.join(2000);
        assert !consumer1.isAlive() : "Consumer did not stop correctly";
    }
}
