package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests concurrent startup and message sending directly after joining. See doc/design/ConcurrentStartupTest.txt
 * for details. This will only work 100% correctly with FLUSH support.<br/>
 * [1] http://jira.jboss.com/jira/browse/JGRP-236
 * @author bela
 */

@Test(groups={Global.FLUSH},sequential=true)
public class ConcurrentStartupTest extends ChannelTestBase {
    private AtomicInteger mod = new AtomicInteger(0);


    public void testConcurrentStartupWithState() {
        final String[] names=new String[] { "A", "B", "C", "D" };
        final int count=names.length;
        final Connector[] channels=new Connector[count];
        final int NUM_MSGS=10;

        try {
            for(int i=0;i < count;i++) {
                if(i == 0)
                    channels[i]=new Connector(createChannel(true, 4, names[i]));
                else
                    channels[i]=new Connector(createChannel(channels[0].getChannel(), names[i]));
                if(i == 0)
                    Util.sleep(1500); // sleep after the first node to reduce the chances of a merge
            }

            // Connect the first channel and establish the initial state by sending a few messages
            channels[0].connect(1,2,3,4,5,6,7);

            // Connect the other channels
            for(int i=1; i < count; i++)
                channels[i].connect(7+i);

            // Make sure everyone is in sync
            Channel[] tmp=new Channel[channels.length];
            for(int i=0; i < channels.length; i++)
                tmp[i]=channels[i].getChannel();

            Util.waitUntilAllChannelsHaveSameSize(30000, 500, tmp);
            System.out.println("\n>>>> all nodes have the same view " + tmp[0].getView() + "  <<<<\n");

            // Sleep to ensure async messages arrive
            System.out.println("Waiting for all channels to have received the " + NUM_MSGS + " messages:");
            long end_time=System.currentTimeMillis() + 10000L;
            while(System.currentTimeMillis() < end_time) {
                boolean terminate=true;
                for(Connector ch: channels) {
                    if(ch.getList().size() != NUM_MSGS) {
                        terminate=false;
                        break;
                    }
                }
                if(terminate)
                    break;
                else
                    Util.sleep(500);
            }

            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
            for(Connector channel:channels)
                System.out.println(channel.getChannel().getName() + ": state=" + channel.getList());
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

            for(Connector ch: channels) {
                List<Integer> list=ch.getList();
                assert list.size() == NUM_MSGS : ": list is " + list + ", should have " + count + " elements";
            }
            System.out.println(">>>> done, all messages received by all channels <<<<\n");
        }
        catch(Exception ex) {
        }
        finally {
            for(Connector connector: channels)
                connector.close();
        }
    }

    protected int getMod() {
        return mod.incrementAndGet();
    }

    

    protected static class Connector extends ReceiverAdapter {
        protected final List<Integer> state=new ArrayList<Integer>(10);
        protected final JChannel      ch;

        public Connector(JChannel ch) {
            this.ch=ch;
        }

        public JChannel getChannel() {
            return ch;
        }

        public void connect(Integer ... numbers) throws Exception {
            ch.setReceiver(this);
            ch.connect("ConcurrentStartupTest", null, 25000); // join and state transfer
            // ch.connect("ConcurrentStartupTest");
            // ch.getState(null, 5000);
            System.out.println(ch.getAddress() + ": --> " + Util.printListWithDelimiter(Arrays.asList(numbers), ","));
            for(int num: numbers)
                ch.send(null, num);
        }

        public void close() {
            Util.close(ch);
        }

        List<Integer> getList() {
            return state;
        }

        public void receive(Message msg) {
            if(msg.getBuffer() == null)
                return;
            Integer number=(Integer)msg.getObject();
            synchronized(state) {
                state.add(number);
                System.out.println(ch.getAddress() + ": <-- " + number + " from " + msg.getSrc() + ", state: " + state);
            }
        }


        public void getState(OutputStream ostream) throws Exception {
            synchronized(state) {
                Util.objectToStream(state, new DataOutputStream(ostream));
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) throws Exception {
            List<Integer> tmp=(List<Integer>)Util.objectFromStream(new DataInputStream(istream));
            synchronized(state) {
                state.clear();
                state.addAll(tmp);
                System.out.println(ch.getAddress() + " <-- state: " + state);
            }
        }
    }
}