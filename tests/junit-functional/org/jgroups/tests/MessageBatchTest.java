package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Tests {@link org.jgroups.util.MessageBatch}
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MessageBatchTest {
    protected static final short UNICAST3_ID=ClassConfigurator.getProtocolId(UNICAST3.class),
      PING_ID=ClassConfigurator.getProtocolId(PING.class),
      FD_ID=ClassConfigurator.getProtocolId(FD_ALL3.class),
      MERGE_ID=ClassConfigurator.getProtocolId(MERGE3.class),
      UDP_ID=ClassConfigurator.getProtocolId(UDP.class);
    protected final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");




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


    public void testIsEmpty() {
        MessageBatch batch=new MessageBatch(3).add(new EmptyMessage()).add(new EmptyMessage()).add(new EmptyMessage());
        assert !batch.isEmpty();
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            it.remove();
        }

        set(batch, 2, new EmptyMessage());
        assert !batch.isEmpty();
    }

    public void testIsEmpty2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        batch.add(new EmptyMessage());
        assert !batch.isEmpty();
        batch.clear();
        assert batch.isEmpty();
        msgs.forEach(batch::add);
        System.out.println("batch = " + batch);
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            it.remove();
        }
        System.out.println("batch = " + batch);
        assert batch.isEmpty();
    }

    public void testCreation() {
        Message[] msgs=createMessages().toArray(new Message[0]);
        int len=msgs.length;
        BatchMessage ebm=new BatchMessage(b, msgs[0].getSrc(), msgs, len);
        assert ebm.getNumberOfMessages() == len;

        ebm=new BatchMessage(b, msgs[0].getSrc(), null, 0);
        assert ebm.getNumberOfMessages() == 0;
        ebm.add(msgs);
        assert ebm.getNumberOfMessages() == len;
    }

    public void testSet() {
        List<Message> msgs=createMessages();
        Message msg=msgs.get(5);
        MessageBatch batch=new MessageBatch(msgs);
        assert get(batch, 5) == msg;
        set(batch, 4,msg);
        assert get(batch, 4) == msg;
    }

    public void testSet2() {
        MessageBatch batch=new MessageBatch(3);
        Message[] msgs=createMessages().toArray(new Message[0]);
        batch.set(b, null, msgs);
        assert batch.size() == msgs.length;
    }


    public void testReplaceIf() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        System.out.println("batch = " + batch);
        int size=batch.size();
        batch.removeIf(msg -> msg.getHeader(UNICAST3_ID) != null, true);
        int removed=size - batch.size();
        System.out.println("batch = " + batch);
        assert batch.size() == size - removed;
    }


    public void testReplaceDuplicates() {
        final Set<Integer> dupes=new HashSet<>(5);
        Predicate<Message> filter=(msg) -> {
            Integer num=msg.getObject();
            return dupes.add(num) == false;
        };

        MessageBatch batch=new MessageBatch(10);
        for(int j=0; j < 2; j++)
            for(int i=1; i <= 5; i++)
                batch.add(new BytesMessage(null, i));
        System.out.println(print(batch));
        assert batch.size() == 10;
        batch.removeIf(filter,  true);
        assert batch.size() == 5;
        System.out.println(print(batch));
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

        msgs.forEach(batch::add);
        System.out.println("batch = " + batch);
        assert batch.size() == prev_size;
        assert batch.capacity() == prev_size;
    }

    public void testRemoveWithFilter() {
        Predicate<Message> filter=msg -> msg != null && msg.isFlagSet(Message.TransientFlag.OOB_DELIVERED);
        MessageBatch batch=new MessageBatch(10);
        for(int i=1; i <= 10; i++) {
            Message msg=new BytesMessage(null, i);
            if(i % 2 == 0)
                msg.setFlag(Message.TransientFlag.OOB_DELIVERED);
            batch.add(msg);
        }
        System.out.println("batch = " + batch);
        assert batch.size() == 10;
        batch.removeIf(filter, true);
        System.out.println("batch = " + batch);
        assert batch.size() == 5;

        for(int i=0; i < 5; i++)
            batch.add(new BytesMessage(null, i).setFlag(Message.TransientFlag.OOB_DELIVERED));
        System.out.println("batch = " + batch);
        batch.removeIf(filter, false);
        assert batch.size() == 9;
    }


    public void testTransfer() {
        MessageBatch other=new MessageBatch(3);
        List<Message> msgs=createMessages();
        msgs.forEach(other::add);
        int other_size=other.size();

        MessageBatch batch=new MessageBatch(5);
        int num=batch.transferFrom(other, true);
        assert num == other_size;
        assert batch.size() == other_size;
        assert other.isEmpty();
    }

    public void testTransfer2() {
        MessageBatch other=new MessageBatch(3);
        List<Message> msgs=createMessages();
        msgs.forEach(other::add);
        int other_size=other.size();

        MessageBatch batch=new MessageBatch(5);
        msgs.forEach(batch::add);
        msgs.forEach(batch::add);
        System.out.println("batch = " + batch);

        int num=batch.transferFrom(other, true);
        assert num == other_size;
        assert batch.size() == other_size;
        assert other.isEmpty();
    }

    public void testTransfer3() {
        MessageBatch other=new MessageBatch(30);
        MessageBatch batch=new MessageBatch(10);
        int num=batch.transferFrom(other, true);
        assert num == 0;
        assert batch.capacity() == 10;
    }

    public void testResize() {
        MessageBatch batch=new MessageBatch(3);
        for(int i=0; i < 3; i++)
            batch.add(new EmptyMessage(null).setSrc(b));
        assert batch.capacity() == 3;
        assert batch.size() == 3;
        batch.resize(10);
        assert batch.capacity() == 10;
        assert batch.size() == 3;
    }

    public void testAdd() {
        MessageBatch batch=new MessageBatch(3);
        List<Message> msgs=createMessages();
        msgs.forEach(batch::add);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() : "batch: " + batch;
    }

    public void testAddBatch() {
        MessageBatch batch=new MessageBatch(3), other=new MessageBatch(3);
        List<Message> msgs=createMessages();
        msgs.forEach(other::add);
        assert other.size() == msgs.size();
        batch.add(other);
        assert batch.size() == msgs.size() : "batch: " + batch;
        assert batch.size() == other.size();
    }

    public void testAddNoResize() {
        MessageBatch batch=new MessageBatch(3);
        List<Message> msgs=createMessages();
        for(int i=0; i < 3; i++)
            batch.add(msgs.get(i));
        assert batch.size() == 3;
        assert batch.capacity() == 3;
        int added=batch.add(msgs.get(3), false);
        assert added == 0 && batch.size() == 3 && batch.capacity() == 3;
    }


    public void testAddBatchNoResizeOK() {
        MessageBatch  batch=new MessageBatch(16);
        List<Message> msgs=createMessages();
        MessageBatch other=new MessageBatch(3);
        msgs.forEach(other::add);
        assert other.size() == msgs.size();
        assert batch.isEmpty();

        int added=batch.add(other, false);
        assert added == other.size();
        assert batch.size() == msgs.size() && batch.capacity() == 16;
        assert other.size() == msgs.size();
    }

    public void testAddBatchNoResizeFail() {
        MessageBatch  batch=new MessageBatch(3);
        List<Message> msgs=createMessages();
        MessageBatch other=new MessageBatch(3);
        msgs.forEach(other::add);
        assert other.size() == msgs.size();
        assert batch.isEmpty();

        int  added=batch.add(other, false);
        assert added == batch.size();
        assert batch.size() == 3 && batch.capacity() == 3;
        assert other.size() == msgs.size();
    }

    public void testAddBatch2() {
        MessageBatch other=new MessageBatch(3);
        List<Message> msgs=createMessages();
        msgs.forEach(other::add);
        int other_size=other.size();

        MessageBatch batch=new MessageBatch(5);
        batch.add(other);
        System.out.println("batch = " + batch);
        assert batch.size() == other_size;
        assert batch.capacity() >= other.capacity();
    }

    public void testAddBatchToItself() {
        MessageBatch batch=new MessageBatch(16);
        for(Message msg: createMessages())
            batch.add(msg);
        try {
            batch.add(batch);
            assert false: "should throw IllegalArumentException as a batch cannot be added to itself";
        }
        catch(IllegalArgumentException ex) {
            System.out.printf("caught %s as expected: %s\n", ex.getClass().getSimpleName(), ex.getCause());
        }
    }


    public void testAddArray() {
        Message[] msgs=createMessages().toArray(new Message[0]);
        MessageBatch mb=new MessageBatch(3);
        int added=mb.add(msgs, msgs.length);
        assert added == msgs.length;
        assert mb.size() == msgs.length;
        assert mb.capacity() >= msgs.length;

        mb=new MessageBatch(3);
        added=mb.add(msgs, 0);
        assert added == 0;
        assert mb.isEmpty();

        mb=new MessageBatch(3);
        added=mb.add(msgs, 5);
        assert added == 5;
        assert mb.size() == 5;
        assert mb.capacity() >= 5;

        mb=new MessageBatch(msgs.length * 3 +1);
        added=0;
        for(int i=0; i < 3; i++)
            added+=mb.add(msgs, msgs.length);
        assert mb.capacity() == msgs.length *3 +1;
        assert mb.size() == msgs.length * 3;
    }

    public void testAnyMatch() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        boolean match=batch.anyMatch(m -> m.getHeader(UDP_ID) != null);
        assert match;

        match=batch.anyMatch(m -> m instanceof ObjectMessage);
        assert !match;

        match=batch.anyMatch(m -> m.getHeader(FD_ID) != null);
        assert match;

        match=batch.anyMatch(m -> m.getHeader((short)111) != null);
        assert !match;
    }


    public void testFirstAndLast() {
        Message first=new EmptyMessage(null), last=new EmptyMessage(null);
        List<Message> msgs=Arrays.asList(new BytesMessage(null), new BytesMessage(null), first,
                                         new ObjectMessage(null), last, new ObjectMessage(null));
        MessageBatch batch=new MessageBatch(msgs);
        assert batch.size() == msgs.size();
        batch.removeIf(m -> !(m instanceof EmptyMessage), true);
        assert batch.size() == 2;
        Message f=batch.first(), l=batch.last();
        assert f == first && l == last;
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
        MessageFactory mf=new DefaultMessageFactory();
        List<Message> msgs=createMessages();
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        Util.writeMessageList(b, a, "cluster".getBytes(), msgs, out, false, UDP_ID);
        out.flush();

        byte[] buf=output.toByteArray();
        System.out.println("size=" + buf.length + " bytes, " + msgs.size() + " messages");

        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buf));
        in.readShort(); // version
        in.readByte(); // flags
        List<Message> list=Util.readMessageList(in, UDP_ID, mf);
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


    public void testStream() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        long num_msgs=batch.stream()
          .filter(msg -> msg.getHeader(UNICAST3_ID) != null)
          .peek(msg -> System.out.printf("msg = %s, hdrs=%s\n", msg, msg.printHeaders()))
          .count();
        System.out.println("num_msgs = " + num_msgs);
        assert num_msgs == 10;

        List<Message> list=batch.stream().collect(Collectors.toList());
        assert list.size() == batch.size();

        int total_size=batch.stream().map(Message::getLength).reduce(0, Integer::sum);
        assert total_size == 0;

        List<Integer> msg_sizes=batch.stream().map(Message::size).collect(Collectors.toList());
        System.out.println("msg_sizes = " + msg_sizes);
        assert msg_sizes.size() == batch.stream().count();
    }


    public void testIterator2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int count=0;
        for(Message ignored : batch)
            count++;
        assert count == msgs.size();

        remove(batch, 3, 5, 10);
        count=0;
        for(Message ignored : batch)
            count++;
        assert count == msgs.size() - 3;
    }

    /** Test removal via iterator */
    public void testIterator3() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg.getHeader(UNICAST3_ID) != null)
                it.remove();
        }
        System.out.println("batch = " + batch);
        assert batch.size() == 3;
     }

    public void testIterator4() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg.getHeader(UNICAST3_ID) != null)
                it.remove();
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
            if(msg != null && msg.getHeader(UNICAST3_ID) != null)
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
        for(Message ignored : batch)
            count++;
        assert count == msgs.size() - 5;
        count=0;
        batch.add(new EmptyMessage()).add(new EmptyMessage());
        System.out.println("batch = " + batch);
        for(Message ignored : batch)
            count++;
        assert count == msgs.size() - 5+2;
    }

    public void testIteratorOnEmptyBatch() {
        MessageBatch batch=new MessageBatch(3);
        int count=0;
        for(Message ignored : batch)
            count++;
        assert count == 0;
    }



    public void testIterator8() {
         List<Message> msgs=createMessages();
         MessageBatch batch=new MessageBatch(msgs);
         int index=0;
         final Message MSG=new EmptyMessage();
        FastArray<Message>.FastIterator it=(FastArray<Message>.FastIterator)batch.iterator();
         while(it.hasNext()) {
             it.next();
             if(index % 2 == 0)
                 it.replace(MSG);
             index++;
         }
         index=0;
         for(Message msg: batch) {
             assert index % 2 != 0 || msg == MSG; // every even index has MSG
             index++;
         }
     }

    public void testIterator9() {
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
        for(Message ignored : batch)
            count++;
        assert count == msgs.size() - 5;
    }


    public void testIterationWithAddition() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        int count=0, added=0;
        for(Message ignored : batch) {
            count++;
            if(count % 2 == 0) {
                batch.add(new EmptyMessage());
                added++;
            }
        }

        System.out.println("batch = " + batch);
        assert count == msgs.size() + added : "the added messages should have been included";

    }


    public void testIterationWithAddition2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        int count=0, added=0;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            it.next();
            count++;
            if(count % 2 == 0) {
                batch.add(new EmptyMessage());
                added++;
            }
        }

        System.out.println("batch = " + batch);
        assert count == msgs.size() + added : "the added messages should *not* have been included";

    }

    public void testIteratorWithFilter() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int count=0;

        Iterator<Message> it=batch.iteratorWithFilter(m -> m.getHeader(PING_ID) != null);
        while(it.hasNext()) {
            it.next();
            count++;
        }
        assert count == 1;

        count=0;
        it=batch.iteratorWithFilter(m -> m.getHeader(UNICAST3_ID) != null);
        while(it.hasNext()) {
            it.next();
            count++;
        }
        assert count == 10;
    }

    public void testRemoveIf() {
        MessageBatch batch=new MessageBatch(10);
        for(int i=0; i < 10; i++)
            batch.add(new BytesMessage(a, i));
        System.out.println("batch = " + batch);
        assert batch.size() == 10;
        batch.removeIf(msg -> { // removes all msgs with even-numbered payloads
            int num=msg.getObject();
            return num % 2 == 0;
        }, true);

        System.out.println("batch = " + batch);
        assert batch.size() == 5;
    }

    public void testDetermineMode() {
        List<Message> l=new ArrayList<>(3);
        for(int i=0; i < 3; i++)
            l.add(new EmptyMessage(null).setFlag(Message.Flag.OOB));
        MessageBatch batch=new MessageBatch(null, null, new AsciiString("cluster"), true, l);
        assert batch.getMode() == MessageBatch.Mode.OOB;
        batch.add(new EmptyMessage(null));
        assert batch.getMode() == MessageBatch.Mode.MIXED;

        batch=new MessageBatch(3);
        batch.add(new EmptyMessage(null));
        assert batch.getMode() == MessageBatch.Mode.REG;
        batch.add(new EmptyMessage(null).setFlag(Message.Flag.OOB));
        assert batch.getMode() == MessageBatch.Mode.MIXED;

        batch=new MessageBatch(3);
        l.add(new EmptyMessage(null));
        batch.add(l);
        assert batch.getMode() == MessageBatch.Mode.MIXED;
    }

    public void testShuffle() {
        List<Message> list=createMessages();
        MessageBatch batch=new MessageBatch(list);
        int old_size=batch.size();
        Message[] msgs=batch.stream().toArray(Message[]::new);
        Util.shuffle(msgs, 0, msgs.length);
        batch.array().set(msgs);
        assert batch.size() == old_size;
    }

    protected static MessageBatch remove(MessageBatch batch, int... indices) {
        for(int index: indices)
            batch.array().remove(index);
        return batch;
    }

    protected static MessageBatch set(MessageBatch batch, int index, Message msg) {
        batch.array().set(index, msg);
        return batch;
    }

    protected static Message get(MessageBatch batch, int index) {
        return batch.array().get(index);
    }

    protected static String print(MessageBatch batch) {
        return batch.stream().map(m -> m.hasPayload()? m.getObject().toString() : "")
          .collect(Collectors.joining(", "));
    }

    protected List<Message> createMessages() {
        List<Message> retval=new ArrayList<>(10);

        for(long seqno=1; seqno <= 5; seqno++)
            retval.add(new EmptyMessage(b).putHeader(UNICAST3_ID, UnicastHeader3.createDataHeader(seqno, (short)22, false)));

        retval.add(new EmptyMessage(b).putHeader(PING_ID, new PingHeader(PingHeader.GET_MBRS_RSP).clusterName("demo-cluster")));
        retval.add(new EmptyMessage(b).putHeader(FD_ID, new FailureDetection.HeartbeatHeader()));
        retval.add(new EmptyMessage(b).putHeader(MERGE_ID, MERGE3.MergeHeader.createViewResponse()));

        for(long seqno=6; seqno <= 10; seqno++)
            retval.add(new EmptyMessage(b).putHeader(UNICAST3_ID, UnicastHeader3.createDataHeader(seqno, (short)22, false)));

        for(Message msg: retval)
            msg.putHeader(UDP_ID, new TpHeader("demo-cluster"));

        return retval;
    }
}
