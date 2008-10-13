package org.jgroups.tests;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Tests FIFO order or messages
 * @author Bela Ban
 * @version $Id: FifoOrderTest.java,v 1.3 2008/10/13 08:10:47 vlada Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class FifoOrderTest extends ChannelTestBase {
    private static final int NUM_MSGS=5000;
    private static final int NUM_NODES=4;
    private final JChannel[] channels=new JChannel[NUM_NODES];

    

    @BeforeMethod
    void setup() throws Exception {
        channels[0]=createChannel(true, 4);
        channels[0].connect("FifoOrderTest-Group");
        for(int i=1; i < channels.length; i++) {
            channels[i]=createChannel(channels[0]);
        }
        for(int i=1; i < channels.length; i++) {
            channels[i].connect("FifoOrderTest-Group");
        }
        for(int i=0; i < channels.length; i++) {
            System.out.println("view[" + i + "]: " + channels[i].getView());
        }
        assert channels[0].getView().size() == NUM_NODES;
    }


    @AfterMethod
    void cleanup() {
        for(int i=channels.length -1; i <=0 ; i--) {
            channels[i].disconnect();
        }
        for(int i=channels.length -1; i <=0 ; i--) {
            channels[i].close();
        }

    }

    
    public void testFIFO() {
        MyReceiver receiver=new MyReceiver();
        for(JChannel channel: channels) {
            channel.setReceiver(receiver);
        }

        Sender[] senders=new Sender[NUM_NODES];
        for(int i=0; i < channels.length; i++) {
            senders[i]=new Sender(i, channels[i]);
            senders[i].start();
        }

        for(Sender sender: senders) {
            try {
                sender.join();
            }
            catch(InterruptedException e) {
            }
        }

        while(true) {
            Util.sleep(2000);
            long[] seqnos=receiver.getSeqnos();
            if(receiver.isDone())
                break;
            System.out.println("seqnos: " + Arrays.toString(seqnos));
        }

        long[] seqnos=receiver.getSeqnos();
        System.out.println("seqnos: " + Arrays.toString(seqnos));
        for(long seqno: seqnos)
            assert seqno == NUM_MSGS;

        if(!receiver.isCorrect()) {
            assert false : receiver.getErrorMessages();
        }
    }


    static class Sender extends Thread {
        final int index;
        final JChannel ch;
        static final int LENGTH=Global.LONG_SIZE + Global.BYTE_SIZE;

        public Sender(int index, JChannel ch) {
            this.index=index;
            this.ch=ch;
        }

        public void run() {
            for(int i=1; i <= NUM_MSGS; i++) {
                ByteBuffer buf=ByteBuffer.allocate(LENGTH);
                buf.put((byte)index); buf.putLong(i);
                buf.rewind();
                Message msg=new Message(null, null, buf.array());
                try {
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Sender #" + index + " done (sent " + NUM_MSGS + " msgs");
        }
    }

    static class MyReceiver extends ReceiverAdapter {
        private final long[] seqnos=new long[NUM_NODES];
        private final StringBuilder sb=new StringBuilder();
        private static final int print=NUM_MSGS / 10;
        private boolean correct=true;

        MyReceiver() {
            for(int i=0; i < seqnos.length; i++)
                seqnos[i]=1;
        }

        public long[] getSeqnos() {
            return seqnos;
        }

        public boolean isCorrect() {
            return correct;
        }

        public String getErrorMessages() {
            return sb.toString();
        }

        public void receive(Message msg) {
            ByteBuffer buf=ByteBuffer.wrap(msg.getBuffer());
            int index=buf.get();
            long new_seqno=buf.getLong();
            Address sender=msg.getSrc();

            long current_seqno=seqnos[index];
            if(current_seqno +1 == new_seqno) {
                seqnos[index]++;
                if(new_seqno % print == 0) {
                    System.out.println(sender + ": " + new_seqno);
                }
            }
            else {
                synchronized(sb) {
                    sb.append(sender + ": ").append(current_seqno).append("\n");
                }
            }
        }


        public boolean isDone() {
            for(long seqno: seqnos) {
                if(seqno < NUM_MSGS)
                    return false;
            }
            return true;
        }
    }


}
