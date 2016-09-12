package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests concurrent startup and message sending directly after joining. See doc/design/ConcurrentStartupTest.txt
 * for details. This will only work 100% correctly with FLUSH support.<br/>
 * [1] http://jira.jboss.com/jira/browse/JGRP-236
 * @author bela
 */

@Test(groups={Global.FLUSH,Global.EAP_EXCLUDED},singleThreaded=true)
public class ConcurrentStartupTest {

    public void testConcurrentStartupWithState() throws Exception {
        final String[] names={ "A", "B", "C", "D" };
        final int      count=names.length;
        final Joiner[] channels=new Joiner[count];
        final int      NUM_MSGS=8;

        try {
            for(int i=0;i < count;i++) {
                channels[i]=new Joiner(createChannel(names[i]));
                if(i == 0)
                    Util.sleep(1500); // sleep after the first node to reduce the chances of a merge
            }

            // Connect the first channel and establish the initial state by sending a few messages
            channels[0].connect(1,2,3,4,5);

            // Connect the other channels
            for(int i=1; i < count; i++)
                channels[i].connect(5+i);

            // Make sure everyone is in sync
            JChannel[] tmp=new JChannel[channels.length];
            for(int i=0; i < channels.length; i++)
                tmp[i]=channels[i].getChannel();

            Util.waitUntilAllChannelsHaveSameView(30000, 500, tmp);
            System.out.println("\n>>>> all nodes have the same view " + tmp[0].getView() + "  <<<<\n");

            // Sleep to ensure async messages arrive
            System.out.println("Waiting for all channels to have received the " + NUM_MSGS + " messages:");
            long end_time=System.currentTimeMillis() + 10000L;
            while(System.currentTimeMillis() < end_time) {
                boolean terminate=true;
                for(Joiner ch: channels) {
                    if(ch.getList().size() != NUM_MSGS) {
                        terminate=false;
                        break;
                    }
                }
                if(terminate)
                    break;
                Util.sleep(500);
            }

            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
            for(Joiner channel:channels)
                System.out.println(channel.getChannel().getName() + ": state=" + channel.getList());
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

            for(Joiner ch: channels) {
                List<Integer> list=ch.getList();
                assert list.size() == NUM_MSGS : ": list is " + list + ", should have " + count + " elements";
            }
            System.out.println(">>>> done, all messages received by all channels <<<<\n");
        }
        finally {
            for(Joiner joiner: channels)
                joiner.close();
        }
    }


    protected JChannel createChannel(String name) throws Exception {
        return new JChannel(Util.getTestStack(new STATE_TRANSFER(), new FLUSH())).name(name);
    }

    protected static class Joiner extends ReceiverAdapter {
        protected final List<Integer> state=new ArrayList<>(10);
        protected final JChannel      ch;

        public Joiner(JChannel ch) {
            this.ch=ch;
        }

        public JChannel      getChannel() {return ch;}
        public List<Integer> getList()    {return state;}
        public void          close()      {Util.close(ch);}

        public void connect(Integer ... numbers) throws Exception {
            ch.setReceiver(this);
            ch.connect("ConcurrentStartupTest", null, 25000); // join and state transfer
            System.out.println(ch.getAddress() + ": --> " + Util.printListWithDelimiter(Arrays.asList(numbers), ","));
            for(int num: numbers)
                ch.send(null, num);
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