package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;

/**
 * Tests {@link org.jgroups.util.MessageBatch}
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MessageBatchTest {
    protected static final short UNICAST2_ID=ClassConfigurator.getProtocolId(UNICAST2.class),
      PING_ID=ClassConfigurator.getProtocolId(PING.class),
      FD_ID=ClassConfigurator.getProtocolId(FD.class),
      MERGE_ID=ClassConfigurator.getProtocolId(MERGE3.class),
      UDP_ID=ClassConfigurator.getProtocolId(UDP.class);
    protected final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");



    public void testCopyConstructor() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() : "batch: " + batch;
        batch.remove(3).remove(6).remove(10);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() -3 : "batch: " + batch;
    }

    public void testCapacityConstructor() {
        MessageBatch batch=new MessageBatch(3);
        assert batch.isEmpty();
    }

    public void testIsEmpty() {
        MessageBatch batch=new MessageBatch(3);
        assert batch.isEmpty();
        batch.set(2, new Message());
        assert !batch.isEmpty();
    }

    public void testGet() {
        List<Message> msgs=createMessages();
        Message msg=msgs.get(5);
        MessageBatch batch=new MessageBatch(msgs);
        assert batch.get(5) == msg;

        for(int index: Arrays.asList(-1, msgs.size(), msgs.size() +1))
            assert batch.get(index) == null;
    }


    public void testSet() {
        List<Message> msgs=createMessages();
        Message msg=msgs.get(5);
        MessageBatch batch=new MessageBatch(msgs);
        assert batch.get(5) == msg;
        batch.set(4, msg);
        assert batch.get(4) == msg;
    }


    public void testRemove() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int prev_size=batch.size();
        batch.remove(1).remove(4);
        System.out.println("batch = " + batch);
        assert batch.size() == prev_size - 2;

        batch.removeAll();
        System.out.println("batch = " + batch);
        assert batch.isEmpty();

        for(Message msg: msgs)
            batch.add(msg);
        System.out.println("batch = " + batch);
        assert batch.size() == prev_size;
        assert batch.capacity() == prev_size;
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
        TP.writeMessageList(b, a, "cluster", msgs, out, false, UDP_ID);
        out.flush();

        byte[] buf=output.toByteArray();
        System.out.println("size=" + buf.length + " bytes, " + msgs.size() + " messages");

        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buf));
        short version=in.readShort();
        byte flags=in.readByte();
        List<Message> list=TP.readMessageList(in, UDP_ID);
        assert msgs.size() == list.size();
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

        batch.remove(3).remove(5).remove(10);
        count=0;
        for(Message msg: batch)
            if(msg != null)
                count++;
        assert count == msgs.size() - 3;
    }

    /** Test removal via iterator */
    public void testIterator3() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        int index=0;
        for(Message msg: batch) {
            if(msg != null && msg.getHeader(UNICAST2_ID) != null)
                batch.remove(index);
            index++;
        }
        System.out.println("batch = " + batch);
        assert batch.size() == 3;
     }

    public void testIterator4() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);

        for(int i=0; i < batch.capacity(); i++) {
            Message msg=batch.get(i);
            if(msg != null && msg.getHeader(UNICAST2_ID) != null)
                batch.remove(i);
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

    public void testIteratorOnEmptyBatch() {
        MessageBatch batch=new MessageBatch(3);
        int count=0;
        for(Message msg: batch)
            if(msg != null)
                count++;

        assert count == 0;
    }



    protected List<Message> createMessages() {
        List<Message> retval=new ArrayList<Message>(10);

        for(long seqno=1; seqno <= 5; seqno++)
            retval.add(new Message(b).putHeader(UNICAST2_ID, UNICAST2.Unicast2Header.createDataHeader(seqno, (short)22, false)));

        retval.add(new Message(b).putHeader(PING_ID, new PingHeader(PingHeader.GET_MBRS_RSP, "demo-cluster")));
        retval.add(new Message(b).putHeader(FD_ID, new FD.FdHeader(org.jgroups.protocols.FD.FdHeader.HEARTBEAT)));
        retval.add(new Message(b).putHeader(MERGE_ID, MERGE3.MergeHeader.createViewResponse(Util.createView(a, 22, a,b))));

        for(long seqno=6; seqno <= 10; seqno++)
            retval.add(new Message(b).putHeader(UNICAST2_ID, UNICAST2.Unicast2Header.createDataHeader(seqno, (short)22, false)));

        for(Message msg: retval)
            msg.putHeader(UDP_ID, new TpHeader("demo-cluster"));

        return retval;
    }
}
