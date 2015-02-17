package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.util.Filter;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests {@link org.jgroups.util.MessageBatch}
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MessageBatchTest {
    protected static final short UNICAST2_ID=ClassConfigurator.getProtocolId(UNICAST2.class),
      PING_ID=ClassConfigurator.getProtocolId(PING.class),
      FD_ID=ClassConfigurator.getProtocolId(FD.class),
      MERGE_ID=ClassConfigurator.getProtocolId(MERGE3.class),
      UDP_ID=ClassConfigurator.getProtocolId(UDP.class);
    protected final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");

    protected static final MessageBatch.Visitor<Integer> print_numbers=new MessageBatch.Visitor<Integer>() {
        public Integer visit(Message msg, MessageBatch batch) {return msg != null? (Integer)msg.getObject() : null;}
    };



    public void testCopyConstructor() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() : "batch: " + batch;
        remove(batch, 3, 6, 10);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() -3 : "batch: " + batch;
    }

    public void testCapacityConstructor() {
        MessageBatch batch=new MessageBatch(3);
        assert batch.isEmpty();
    }

    public void testCreationWithFilter() {
        List<Message> msgs=new ArrayList<>(10);
        for(int i=1; i <= 10; i++)
            msgs.add(new Message(null, i));
        MessageBatch batch=new MessageBatch(null, null, null, true, msgs, new Filter<Message>() {
            public boolean accept(Message msg) {
                return msg != null && ((Integer)msg.getObject()) % 2 == 0; // only even numbers are accepted
            }
        });
        System.out.println(batch.map(print_numbers));
        assert batch.size() == 5;
        for(Message msg: batch)
            assert ((Integer)msg.getObject()) % 2 == 0;
    }


    public void testCreationWithFilter2() {
        List<Message> msgs=new ArrayList<>(20);
        for(int i=1; i <= 20; i++) {
            Message msg=new Message(null, i);
            if(i <= 10) {
                msg.setFlag(Message.Flag.OOB);
                if(i % 2 == 0)
                    msg.setTransientFlag(Message.TransientFlag.OOB_DELIVERED);
            }
            msgs.add(msg);
        }

        MessageBatch batch=new MessageBatch(null, null, null, true, msgs, new Filter<Message>() {
            public boolean accept(Message msg) {
                return msg != null && (!msg.isFlagSet(Message.Flag.OOB) || msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED));
                // return msg != null && !(msg.isFlagSet(Message.Flag.OOB) && !msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED));
            }
        });

        System.out.println("batch = " + batch.map(print_numbers));
        assert batch.size() == 15;
        for(Message msg: batch) {
            int num=(Integer)msg.getObject();
            if(num <= 10)
                assert msg.isTransientFlagSet(Message.TransientFlag.OOB_DELIVERED);
        }
    }


    public void testIsEmpty() {
        MessageBatch batch=new MessageBatch(3).add(new Message()).add(new Message()).add(new Message());
        assert !batch.isEmpty();
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            it.remove();
        }

        set(batch, 2, new Message());
        assert !batch.isEmpty();
    }

    public void testIsEmpty2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        batch.add(new Message());
        assert !batch.isEmpty();
        batch.clear();
        assert batch.isEmpty();
        for(Message msg: msgs)
            batch.add(msg);
        System.out.println("batch = " + batch);
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            it.remove();
        }
        System.out.println("batch = " + batch);
        assert batch.isEmpty();
    }


    public void testSet() {
        List<Message> msgs=createMessages();
        Message msg=msgs.get(5);
        MessageBatch batch=new MessageBatch(msgs);
        assert get(batch, 5) == msg;
        set(batch, 4,msg);
        assert get(batch, 4) == msg;
    }


    public void testReplace() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        final Message MSG=new Message();

        int index=0;
        for(Message msg: batch) {
            if(index % 2 == 0)
                batch.replace(msg,MSG);
            index++;
        }

        index=0;
        for(Message msg: batch) {
            if(index % 2 == 0)
                assert msg == MSG; // every even index has MSG
            index++;
        }
    }


    public void testReplace2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        final Message MSG=new Message();
        batch.replace(MSG, null);
        assert batch.size() == msgs.size(); // MSG was *not* found and therefore *not* nulled

        batch.replace(get(batch, 5), null);
        assert batch.size() == msgs.size() -1;
    }

    public void testReplace3() {
        MessageBatch batch=new MessageBatch(1).add(new Message(null, "Bela")).add(new Message(null, "Michi"))
          .add(new Message(null, "Nicole"));
        System.out.println("batch = " + batch);
        for(Message msg: batch) {
            if("Michi".equals(msg.getObject())) {
                msg.setObject("Michelle");
                batch.replace(msg, msg); // tests replacing the message with itself (with changed buffer though)
            }
        }
        Queue<String> names=new LinkedBlockingQueue<>(Arrays.asList("Bela", "Michelle", "Nicole"));
        for(Message msg: batch) {
            String expected=names.poll();
            String name=(String)msg.getObject();
            System.out.println("found=" + name + ", expected=" + expected);
            assert name.equals(expected) : "found=" + name + ", expected=" + expected;
        }
    }


    public void testReplaceDuplicates() {
        Filter<Message> filter=new Filter<Message>() {
            protected final Set<Integer> dupes=new HashSet<>(5);
            public boolean accept(Message msg) {
                Integer num=(Integer)msg.getObject();
                return dupes.add(num) == false;
            }
        };

        MessageBatch batch=new MessageBatch(10);
        for(int j=0; j < 2; j++)
            for(int i=1; i <= 5; i++)
                batch.add(new Message(null, i));
        System.out.println(batch.map(print_numbers));
        assert batch.size() == 10;
        batch.replace(filter,  null, true);
        assert batch.size() == 5;
        System.out.println(batch.map(print_numbers));
    }


    public void testRemove() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int prev_size=batch.size();
        remove(batch, 1, 4);
        System.out.println("batch = " + batch);
        assert batch.size() == prev_size - 2;

        batch.clear();
        System.out.println("batch = " + batch);
        assert batch.isEmpty();

        for(Message msg: msgs)
            batch.add(msg);
        System.out.println("batch = " + batch);
        assert batch.size() == prev_size;
        assert batch.capacity() == prev_size;
    }

    public void testRemoveWithFilter() {
        Filter<Message> filter=new Filter<Message>() {
            public boolean accept(Message msg) {return msg != null && msg.isTransientFlagSet(Message.TransientFlag.OOB_DELIVERED);}
        };
        MessageBatch batch=new MessageBatch(10);
        for(int i=1; i <= 10; i++) {
            Message msg=new Message(null, i);
            if(i % 2 == 0)
                msg.setTransientFlag(Message.TransientFlag.OOB_DELIVERED);
            batch.add(msg);
        }
        System.out.println("batch = " + batch);
        assert batch.size() == 10;
        batch.remove(filter);
        System.out.println("batch = " + batch);
        assert batch.size() == 5;

        for(int i=0; i < 5; i++)
            batch.add(new Message(null, i).setTransientFlag(Message.TransientFlag.OOB_DELIVERED));
        System.out.println("batch = " + batch);
        batch.replace(filter, null, false);
        assert batch.size() == 9;
    }


    public void testAdd() {
        MessageBatch batch=new MessageBatch(3);
        List<Message> msgs=createMessages();
        for(Message msg: msgs)
            batch.add(msg);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() : "batch: " + batch;
    }

    public void testGetMatchingMessages() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        Collection<Message> matching=batch.getMatchingMessages(UDP_ID, false);
        assert matching.size() == batch.size();
        assert batch.size() == msgs.size();

        matching=batch.getMatchingMessages(FD_ID, true);
        assert matching.size() == 1;
        assert batch.size() == msgs.size() -1;

        int size=batch.size();
        matching=batch.getMatchingMessages(UDP_ID, true);
        assert matching.size() == size;
        assert batch.isEmpty();
    }


    public void testTotalSize() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        long total_size=0;
        for(Message msg: msgs)
            total_size+=msg.size();
        System.out.println("total size=" + batch.totalSize());
        assert batch.totalSize() == total_size;
    }


    public void testSize() throws Exception {
        List<Message> msgs=createMessages();
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        TP.writeMessageList(b, a, "cluster".getBytes(), msgs, out, false, UDP_ID);
        out.flush();

        byte[] buf=output.toByteArray();
        System.out.println("size=" + buf.length + " bytes, " + msgs.size() + " messages");

        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buf));
        in.readShort(); // version
        in.readByte(); // flags
        List<Message> list=TP.readMessageList(in, UDP_ID);
        assert msgs.size() == list.size();
    }

    public void testSize2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        assert batch.size() == msgs.size();
        remove(batch, 2, 3, 10);
        assert batch.size() == msgs.size() - 3;
    }


    public void testIterator() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int index=0, count=0;
        for(Message msg: batch) {
            Message tmp=msgs.get(index++);
            count++;
            assert msg == tmp;
        }
        assert count == msgs.size();
    }


    public void testIterator2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int count=0;
        for(Message msg: batch)
            count++;
        assert count == msgs.size();

        remove(batch, 3, 5, 10);
        count=0;
        for(Message msg: batch)
            count++;
        assert count == msgs.size() - 3;
    }

    /** Test removal via iterator */
    public void testIterator3() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        for(Message msg: batch)
            if(msg.getHeader(UNICAST2_ID) != null)
                batch.remove(msg);
        System.out.println("batch = " + batch);
        assert batch.size() == 3;
     }

    public void testIterator4() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        for(Message msg: batch) {
            if(msg.getHeader(UNICAST2_ID) != null)
                batch.remove(msg);
        }
        System.out.println("batch = " + batch);
        assert batch.size() == 3;
    }

    public void testIterator5() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        Iterator<Message> itr=batch.iterator();
        itr.remove();
        assert batch.size() == msgs.size(); // didn't remove anything

        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg != null && msg.getHeader(UNICAST2_ID) != null)
                it.remove();
        }
        System.out.println("batch = " + batch);
        assert batch.size() == 3;
    }

    public void testIterator6() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        remove(batch, 1, 2, 3, 10, msgs.size()-1);
        System.out.println("batch = " + batch);
        int count=0;
        for(Message msg: batch)
            count++;
        assert count == msgs.size() - 5;
        count=0;
        batch.add(new Message()).add(new Message());
        System.out.println("batch = " + batch);
        for(Message msg: batch)
            count++;
        assert count == msgs.size() - 5+2;
    }

    public void testIteratorOnEmptyBatch() {
        MessageBatch batch=new MessageBatch(3);
        int count=0;
        for(Message msg: batch)
            count++;
        assert count == 0;
    }


    public void testIterator7() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int index=0;
        final Message MSG=new Message();
        for(Message msg: batch) {
            if(index % 2 == 0)
                batch.replace(msg, MSG);
            index++;
        }

        index=0;
        for(Message msg: batch) {
            if(index % 2 == 0)
                assert msg == MSG; // every even index has MSG
            index++;
        }
    }

    public void testIterator8() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int index=0;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            if(index == 1 || index == 2 || index == 3 || index == 10 || index == msgs.size()-1)
                it.remove();
            index++;
        }
        System.out.println("batch = " + batch);
        int count=0;
        for(Message msg: batch)
            count++;
        assert count == msgs.size() - 5;
    }


    public void testIterationWithAddition() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        int count=0;
        for(Message msg: batch) {
            count++;
            if(count % 2 == 0)
                batch.add(new Message());
        }

        System.out.println("batch = " + batch);
        assert count == msgs.size() : "the added messages should *not* have been included";

    }


    public void testIterationWithAddition2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        int count=0;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            count++;
            if(count % 2 == 0)
                batch.add(new Message());
        }

        System.out.println("batch = " + batch);
        assert count == msgs.size() : "the added messages should *not* have been included";

    }


    protected MessageBatch remove(MessageBatch batch, int ... indices) {
        Message[] msgs=batch.array();
        for(int index: indices)
            msgs[index]=null;
        return batch;
    }

    protected MessageBatch set(MessageBatch batch, int index, Message msg) {
        Message[] msgs=batch.array();
        msgs[index]=msg;
        return batch;
    }

    protected Message get(MessageBatch batch, int index) {
        return batch.array()[index];
    }


    protected List<Message> createMessages() {
        List<Message> retval=new ArrayList<>(10);

        for(long seqno=1; seqno <= 5; seqno++)
            retval.add(new Message(b).putHeader(UNICAST2_ID, UNICAST2.Unicast2Header.createDataHeader(seqno, (short)22, false)));

        retval.add(new Message(b).putHeader(PING_ID, new PingHeader(PingHeader.GET_MBRS_RSP).clusterName("demo-cluster")));
        retval.add(new Message(b).putHeader(FD_ID, new FD.FdHeader(org.jgroups.protocols.FD.FdHeader.HEARTBEAT)));
        retval.add(new Message(b).putHeader(MERGE_ID, MERGE3.MergeHeader.createViewResponse()));

        for(long seqno=6; seqno <= 10; seqno++)
            retval.add(new Message(b).putHeader(UNICAST2_ID, UNICAST2.Unicast2Header.createDataHeader(seqno, (short)22, false)));

        for(Message msg: retval)
            msg.putHeader(UDP_ID, new TpHeader("demo-cluster"));

        return retval;
    }
}
