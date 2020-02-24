package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Message;
import org.jgroups.util.NonBlockingCredit;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Tests {@link org.jgroups.util.NonBlockingCredit}
 * @author Bela Ban
 * @since  4.0.4
 */
@Test
public class NonBlockingCreditTest {
    protected static final Address dest=Util.createRandomAddress("A");
    protected static final int max_credits=10000;

    public void testDecrement() {
        MessageSender msg_sender=new MessageSender();
        NonBlockingCredit cred=new NonBlockingCredit(max_credits, 500_000, new ReentrantLock(), msg_sender);
        Message msg=msg(1000);
        boolean success=cred.decrementIfEnoughCredits(msg, msg.getLength(), 500);
        assert success && cred.get() == 9000;
        cred.increment(2000, max_credits);
        assert !cred.isQueuing() && cred.get() == max_credits;
    }

    public void testDecrementAndQueing() {
        MessageSender msg_sender=new MessageSender();
        NonBlockingCredit cred=new NonBlockingCredit(max_credits, 500_000, new ReentrantLock(), msg_sender);
        Message msg=msg(1000);
        boolean success=cred.decrementIfEnoughCredits(msg, msg.getLength(), 500);
        assert success && cred.get() == 9000;

        msg=msg(9000);
        success=cred.decrementIfEnoughCredits(msg, msg.getLength(), 500);
        assert success && cred.get() == 0;

        for(int i=0; i < 5; i++) {
            msg=msg(1000);
            success=cred.decrementIfEnoughCredits(msg, msg.getLength(), 500);
            assert !success && cred.get() == 0;
        }

        assert cred.isQueuing();
        assert cred.getQueuedMessages() == 5;
        assert cred.getQueuedMessageSize() == 5000;

        cred.increment(500, max_credits); // not enough to trigger a message send
        assert cred.isQueuing();
        assert cred.get() == 500 && cred.getQueuedMessages() == 5 && cred.getQueuedMessageSize() == 5000;

        cred.increment(500, max_credits); // this is enough to send the first queued message
        assert cred.isQueuing();
        assert cred.get() == 0 && cred.getQueuedMessages() == 4 && cred.getQueuedMessageSize() == 4000;
        assert msg_sender.sent_msgs == 1;

        cred.increment(20000, max_credits);
        assert !cred.isQueuing();
        assert cred.get() == 6000 && cred.getQueuedMessages() == 0 && cred.getQueuedMessageSize() == 0;
        assert msg_sender.sent_msgs == 5;
    }

    /**
     * Sender S blocks on the full queue, then another thread applies credits: S should unblock
     */
    public void testDecrementAndBlockingOnFullQueue() {
        MessageSender msg_sender=new MessageSender();
        final NonBlockingCredit cred=new NonBlockingCredit(2500, 1500, new ReentrantLock(), msg_sender);
        final AtomicInteger count=new AtomicInteger();
        new Thread(() -> {
            Util.sleep(2000);
            System.out.printf("[%s] adding 10000 credits\n", Thread.currentThread());
            cred.increment(10000, 10000);
        }).start();

        for(int i=0; i < 10; i++) {
            Message msg=msg(1000);
            cred.decrementIfEnoughCredits(msg, msg.getLength(), 2000);
            count.incrementAndGet();
        }
        System.out.printf("received %d msgs", count.get());
        assert count.get() == 10;
    }

    /** Tests sender threads being blocked and then unblocked by calling clear() */
    public void testQueueing() throws TimeoutException {
        final NonBlockingCredit cred=new NonBlockingCredit(10000, 5000, new ReentrantLock(), null);
        Thread[] senders=new Thread[10];
        for(int i=0; i < senders.length; i++) {
            Message msg=msg(1000);
            senders[i]=new Thread(() -> cred.decrementIfEnoughCredits(msg, msg.getLength(), 0));
        }

        Message msg=msg(10000);
        boolean rc=cred.decrementIfEnoughCredits(msg, msg.getLength(), 500);
        assert rc;
        assert !cred.isQueuing();

        // Now fill the queue
        for(int i=0; i < 5; i++)
            assert !cred.decrementIfEnoughCredits(msg(1000), 1000, 500);
        assert cred.isQueuing();
        assert cred.getQueuedMessages() == 5;
        assert cred.getQueuedMessageSize() == 5000;

        for(Thread t: senders)
            t.start();

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(senders).allMatch(t -> t.getState() == Thread.State.WAITING),
                       () -> "threads:\n" + Arrays.stream(senders).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(senders).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
        assert cred.isQueuing() && cred.getQueuedMessages() == 5;

        cred.reset();
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(senders).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(senders).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(senders).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
    }


    protected static Message msg(int size) {
        byte[] buf=new byte[size];
        return new BytesMessage(dest, buf);
    }

    protected static class MessageSender implements Consumer<Message> {
        protected int sent_msgs;

        public int getSentMessages() {return sent_msgs;}

        public void accept(Message message) {
            sent_msgs++;
        }
    }
}
