package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ArrayIterator;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests correct state transfer while other members continue sending messages to the group
 * @author Bela Ban
 */
@Test(groups={Global.STACK_DEPENDENT,Global.EAP_EXCLUDED},singleThreaded=true)
public class StateTransferTest extends ChannelTestBase {
    static final int                     MSG_SEND_COUNT=1000;
    static final String[]                names= {"A", "B", "C", "D"};
    static final int                     APP_COUNT=names.length;
    static final Class<?>[]              NAK_PROTS={NAKACK2.class,NAKACK.class};
    static final short[]                 ids=new short[NAK_PROTS.length];
    protected StateTransferApplication[] apps=new StateTransferApplication[APP_COUNT];


    static {
        for(int i=0; i < NAK_PROTS.length; i++)
            ids[i]=ClassConfigurator.getProtocolId(NAK_PROTS[i]);
    }

    @DataProvider(name="createChannels")
    protected Iterator<Class<?>[]> createChannels() {
        return new ArrayIterator<>(new Class<?>[][]{{STATE_TRANSFER.class}, {STATE.class}, {STATE_SOCK.class}});
    }


    @AfterMethod protected void destroy() {
        for(StateTransferApplication app: apps)
            if(app != null) {
                app.getChannel().setReceiver(null);
                app.cleanup();
            }
    }

    @Test(dataProvider="createChannels")
    public void testStateTransferFromSelfWithRegularChannel(final Class<?> state_transfer_class) throws Exception {
        JChannel ch=createChannel(true);
        replaceStateTransferProtocolWith(ch, state_transfer_class);
        ch.connect("StateTransferTest");
        try {
            Address self=ch.getAddress();
            assert self != null;
            ch.getState(self, 20000);
            assert true : "getState() on self should return";
        }
        finally {
            Util.close(ch);
        }
    }

    // @Test(dataProvider="createChannels",invocationCount=10)
    @Test(dataProvider="createChannels")
    public void testStateTransferWhileSending(final Class<?> state_transfer_class) throws Exception {
        Semaphore semaphore=new Semaphore(APP_COUNT, true); // fifo order
        semaphore.acquire(APP_COUNT);
        Thread[] threads=new Thread[APP_COUNT];

        int from=0, to=MSG_SEND_COUNT;
        for(int i=0;i < apps.length;i++) {
            if(i == 0)
                apps[i]=new StateTransferApplication(semaphore, names[i], from, to);
            else
                apps[i]=new StateTransferApplication(apps[0].getChannel(), semaphore, names[i], from, to);
            replaceStateTransferProtocolWith(apps[i].getChannel(), state_transfer_class);
            threads[i]=new Thread(apps[i], "thread-" + names[i]);
            threads[i].start();
            from+=MSG_SEND_COUNT;
            to+=MSG_SEND_COUNT;
        }

        for(int i=0;i < threads.length; i++) {
            semaphore.release();
            Util.sleep(i == 0? 4000 : 100); // to reduce changes of a merge
        }

        // Make sure everyone is in sync
        Channel[] tmp=new Channel[apps.length];
        for(int i=0; i < apps.length; i++)
            tmp[i]=apps[i].getChannel();

        Util.waitUntilAllChannelsHaveSameSize(20000, 1000, tmp);

        for(Thread thread: threads)
            thread.join(20000);
        for(Thread thread: threads)
            if(thread.isAlive())
                throw new Exception("Thread " + thread.getName() + " is still alive");

        // Sleep to ensure async messages arrive
        System.out.println("Waiting for all channels to have " + MSG_SEND_COUNT * APP_COUNT + " elements:");
        long end_time=System.currentTimeMillis() + 20000L;
        while(System.currentTimeMillis() < end_time) {
            boolean terminate=true;
            for(StateTransferApplication app: apps) {
                Map<String,List<Long>> map=app.getMap();
                if(getSize(map) != MSG_SEND_COUNT * APP_COUNT) {
                    terminate=false;
                    break;
                }
            }
            if(terminate)
                break;
            else {
                resumeStableAndGC();
                Util.sleep(500);
            }
        }

        for(int i=0;i < apps.length;i++) {
            StateTransferApplication w=apps[i];
            ConcurrentMap<Address,AtomicInteger> map=w.getCount();
            System.out.println("msgs for " + names[i] + ":");
            for(Map.Entry<Address,AtomicInteger> entry: map.entrySet())
                System.out.println("from " + entry.getKey() + " --> " + entry.getValue() + " msgs");
        }

        // have we received all and the correct messages?
        System.out.println("++++++++++++++++++++++++++++++++++++++");
        for(int i=0;i < apps.length;i++) {
            StateTransferApplication w=apps[i];
            Map<String,List<Long>> m=w.getMap();
            System.out.println("\n" + names[i] + " (" + getSize(m) + "): digest=" + w.getChannel().down(Event.GET_DIGEST_EVT));
            for(String name: names)
                System.out.println("map " + name + ": " + print(m.get(name)));
        }
        System.out.println("++++++++++++++++++++++++++++++++++++++");


        for(int i=0;i < apps.length;i++) {
            StateTransferApplication w=apps[i];
            Map<String,List<Long>> m=w.getMap();
            assert getSize(m) == MSG_SEND_COUNT * APP_COUNT : "map " + names[i] + " has " + getSize(m) +
              " elements (expected: " + MSG_SEND_COUNT * APP_COUNT + ")";
        }

        // compare the values
        for(String name: names) {
            List<Long> list=apps[0].getMap().get(name);
            for(int i=1; i < apps.length; i++) {
                StateTransferApplication app=apps[i];
                List<Long> other_list=app.getMap().get(name);
                assert list.equals(other_list);
            }
        }
    }


    protected void resumeStableAndGC() {
        for(StateTransferApplication app: apps) {
            STABLE stable=(STABLE)app.getChannel().getProtocolStack().findProtocol(STABLE.class);
            stable.down(new Event(Event.RESUME_STABLE));
            stable.gc();
        }
    }


    protected String print(List<Long> list) {
        if(list.isEmpty())
            return "[] (0 elements)";
        long first=list.get(0);
        int size=list.size();
        long last=list.get(size-1);
        return "[" + first + " .. " + last + "] (" + size + " elements)";
    }

    protected int getSize(Map<String,List<Long>> map) {
        int retval=0;
        for(List<Long> list: map.values())
            retval+=list.size();
        return retval;
    }

    protected long getSeqno(Message msg) {
        for(short id: ids) {
            Header hdr=msg.getHeader(id);
            if(hdr != null)
                return getSeqnoFromHeader(hdr);
        }
        return -1;
    }

    protected long getSeqnoFromHeader(Header hdr) {
        Field field=Util.getField(hdr.getClass(), "seqno");
        return (Long)Util.getField(field, hdr);
    }


    protected void replaceStateTransferProtocolWith(JChannel ch, Class<?> state_transfer_class) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        if(stack.findProtocol(state_transfer_class) != null)
            return; // protocol of the right class is already in stack
        Protocol prot=stack.findProtocol(STATE_TRANSFER.class, StreamingStateTransfer.class);
        Protocol new_state_transfer_protcol=(Protocol)state_transfer_class.newInstance();
        if(prot != null) {
            stack.replaceProtocol(prot, new_state_transfer_protcol);
        }
        else { // no state transfer protocol found in stack
            Protocol flush=stack.findProtocol(FLUSH.class);
            if(flush != null)
                stack.insertProtocol(new_state_transfer_protcol, ProtocolStack.BELOW, FLUSH.class);
            else
                stack.insertProtocolAtTop(new_state_transfer_protcol);
        }
    }


    protected class StateTransferApplication extends ReceiverAdapter implements Runnable {
        protected final Map<String,List<Long>>         map=new HashMap<>(MSG_SEND_COUNT * APP_COUNT);
        protected final int                            from, to;
        protected ConcurrentMap<Address,AtomicInteger> count=new ConcurrentHashMap<>();
        protected final Semaphore                      semaphore;
        protected final JChannel                       channel;
        protected long                                 start_time;

        public StateTransferApplication(Semaphore semaphore, String name, int from, int to) throws Exception {
            this.from=from;
            this.to=to;
            this.semaphore=semaphore;
            init();
            channel=createChannel(true, APP_COUNT, name);
            channel.setReceiver(this);

        }
        
        public StateTransferApplication(JChannel copySource, Semaphore semaphore, String name, int from, int to) throws Exception {
            this.from=from;
            this.to=to;
            this.semaphore=semaphore;
            init();
            this.channel=createChannel(copySource, name);
            channel.setReceiver(this);

        }

        protected void init() {
            for(String s: names)
                map.put(s, new ArrayList<Long>(MSG_SEND_COUNT * APP_COUNT));
        }

        public JChannel getChannel() {
            return channel;
        }

        public void cleanup() {Util.close(channel);}

        public Map<String,List<Long>> getMap() {
            synchronized(map) {
                return map;
            }
        }

        public ConcurrentMap<Address,AtomicInteger> getCount() {
            return count;
        }

        public void receive(Message msg) {
            String key=(String)msg.getObject();

            Address sender=msg.getSrc();
            AtomicInteger cnt=count.get(sender);
            if(cnt == null) {
                cnt=new AtomicInteger(0);
                AtomicInteger tmp=count.putIfAbsent(sender,cnt);
                if(tmp != null)
                    cnt=tmp;
            }
            cnt.incrementAndGet();

            long seqno=getSeqno(msg);
            if(seqno == -1)
                throw new IllegalArgumentException("NAKACK{2} seqno could not be fetched from message");

            synchronized(map) {
                List<Long> list=map.get(key);
                // needed because we might get retransmissions of messages that are already in the state !
                if(!list.contains(seqno))
                    list.add(seqno);
            }
        }


        public void getState(OutputStream ostream) throws Exception {
            OutputStream out=new BufferedOutputStream(ostream);
            synchronized(map) {
                Util.objectToStream(map, new DataOutputStream(out));
                out.flush();
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) throws Exception {
            Map<String,List<Long>> tmp=(Map<String,List<Long>>)Util.objectFromStream(new DataInputStream(istream));
            synchronized(map) {
                map.clear();
                map.putAll(tmp);
                count.clear();
                long time=System.currentTimeMillis() - start_time;
                StringBuilder sb=new StringBuilder("\n++++++++++++++++++++++++++++++++++++++\n");
                sb.append(channel.getAddress() + " <--- received state (in " + time + " ms), map has "
                            + getSize(map) + " elements:\n");
                for(Map.Entry<String,List<Long>> entry: map.entrySet())
                    sb.append(entry.getKey() + ": " + print(entry.getValue()) + "\n");
                sb.append("++++++++++++++++++++++++++++++++++++++");
                System.out.println(sb);
            }
        }

        public void run() {
            boolean acquired=false;
            try {
                acquired=semaphore.tryAcquire(60000L, TimeUnit.MILLISECONDS);
                if(!acquired)
                    throw new Exception(channel.getAddress() + " cannot acquire semaphore");
                useChannel();
            }
            catch(Exception e) {
                log.error(channel.getAddress() + ": ", e);
            }
        }


        protected void useChannel() throws Exception {
            start_time=System.currentTimeMillis();
            channel.connect("StateTransferTest", null, 20000);

            int cnt=0;
            for(int i=from; i < to; i++) {
                try {
                    channel.send(null, channel.getName()); // the receiver uses name as key and the seqno of NAKACK{2} as value
                    cnt++;
                    if(cnt % 100 == 0)
                        Util.sleep(50);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


}