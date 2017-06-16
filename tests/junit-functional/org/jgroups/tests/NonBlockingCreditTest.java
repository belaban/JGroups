package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.NonBlockingCredit;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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
        boolean success=cred.decrementIfEnoughCredits(msg, msg.length(), 500);
        assert success && cred.get() == 9000;
        cred.increment(2000, max_credits);
        assert !cred.isQueuing() && cred.get() == max_credits;
    }

    public void testDecrementAndQueing() {
        MessageSender msg_sender=new MessageSender();
        NonBlockingCredit cred=new NonBlockingCredit(max_credits, 500_000, new ReentrantLock(), msg_sender);
        Message msg=msg(1000);
        boolean success=cred.decrementIfEnoughCredits(msg, msg.length(), 500);
        assert success && cred.get() == 9000;

        msg=msg(9000);
        success=cred.decrementIfEnoughCredits(msg, msg.length(), 500);
        assert success && cred.get() == 0;

        for(int i=0; i < 5; i++) {
            msg=msg(1000);
            success=cred.decrementIfEnoughCredits(msg, msg.length(), 500);
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


    protected static Message msg(int size) {
        byte[] buf=new byte[size];
        return new Message(dest, buf);
    }

    protected static class MessageSender implements Consumer<Message> {
        protected int sent_msgs;

        public int getSentMessages() {return sent_msgs;}

        public void accept(Message message) {
            sent_msgs++;
        }
    }
}
