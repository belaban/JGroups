

package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests a SEQUENCER based stack: A, B and C. B starts multicasting messages with a monotonically increasing
 * number. Then A is crashed. C and B should receive *all* numbers *without* a gap.
 * @author Bela Ban
 * @version $Id: SequencerFailoverTest.java,v 1.8 2008/04/08 08:29:34 belaban Exp $
 */
public class SequencerFailoverTest {
    JChannel ch1, ch2, ch3; // ch1 is the coordinator
    static final String GROUP="demo-group";
    static final int NUM_MSGS=50;


    String props="sequencer.xml";



    public SequencerFailoverTest(String name) {
    }

    @BeforeMethod
    public void setUp() throws Exception {
        ;
        ch1=new JChannel(props);
        ch1.connect(GROUP);

        ch2=new JChannel(props);
        ch2.connect(GROUP);

        ch3=new JChannel(props);
        ch3.connect(GROUP);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        ;
        if(ch3 != null) {
            ch3.close();
            ch3 = null;
        }
        if(ch2 != null) {
            ch2.close();
            ch2 = null;
        }
    }

    @Test
    public void testBroadcastSequence() throws Exception {
        MyReceiver r2=new MyReceiver(), r3=new MyReceiver();
        ch2.setReceiver(r2); ch3.setReceiver(r3);

        View v2=ch2.getView(), v3=ch3.getView();
        System.out.println("ch2's view: " + v2 + "\nch3's view: " + v3);
        Assert.assertEquals(v2, v3);

        new Thread() {
            public void run() {
                Util.sleep(3000);
                System.out.println("** killing ch1");
                ch1.shutdown(); ch1=null;
                System.out.println("** ch1 killed");
            }
        }.start();

        for(int i=1; i <= NUM_MSGS; i++) {
            Util.sleep(300);
            ch2.send(new Message(null, null, new Integer(i)));
            System.out.print("-- messages sent: " + i + "/" + NUM_MSGS + "\r");
        }
        System.out.println("");
        v2=ch2.getView();
        v3=ch3.getView();
        System.out.println("ch2's view: " + v2 + "\nch3's view: " + v3);
        Assert.assertEquals(v2, v3);

        Assert.assertEquals(2, v2.size());
        int s2, s3;
        for(int i=15000; i > 0; i-=1000) {
            s2=r2.size(); s3=r3.size();
            if(s2 >= NUM_MSGS && s3 >= NUM_MSGS) {
                System.out.print("ch2: " + s2 + " msgs, ch3: " + s3 + " msgs\r");
                break;
            }
            Util.sleep(1000);
            System.out.print("sleeping for " + (i/1000) + " seconds (ch2: " + s2 + " msgs, ch3: " + s3 + " msgs)\r");
        }
        System.out.println("-- verifying messages on ch2 and ch3");
        verifyNumberOfMessages(NUM_MSGS, r2);
        verifyNumberOfMessages(NUM_MSGS, r3);
    }

    private static void verifyNumberOfMessages(int num_msgs, MyReceiver receiver) throws Exception {
        List<Integer> msgs=receiver.getList();
        System.out.println("list has " + msgs.size() + " msgs (should have " + NUM_MSGS + ")");
        Assert.assertEquals(num_msgs, msgs.size());
        int i=1;
        for(Integer tmp: msgs) {
            if(tmp != i)
                throw new Exception("expected " + i + ", but got " + tmp);
            i++;
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        List<Integer> list=new LinkedList<Integer>();

        public List<Integer> getList() {
            return list;
        }

        public int size() {return list.size();}

        public void receive(Message msg) {
            list.add((Integer)msg.getObject());
        }

        void clear() {
            list.clear();
        }

//        public void viewAccepted(View new_view) {
//            System.out.println("** view: " + new_view);
//        }
    }



}
