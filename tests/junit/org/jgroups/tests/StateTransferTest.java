package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests correct state transfer while other members continue sending messages to the group
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class StateTransferTest extends ChannelTestBase {
    static final int MSG_SEND_COUNT=5000;
    static final String[] names= { "A", "B", "C", "D"};
    static final int APP_COUNT=names.length;

    
    public void testStateTransferFromSelfWithRegularChannel() throws Exception {
        Channel ch=createChannel(true);
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


    public void testStateTransferWhileSending() throws Exception {

        StateTransferApplication[] apps=new StateTransferApplication[APP_COUNT];
        try {
            Semaphore semaphore=new Semaphore(APP_COUNT);
            semaphore.acquire(APP_COUNT);

            int from=0, to=MSG_SEND_COUNT;
            for(int i=0;i < apps.length;i++) {
                if(i == 0)
                    apps[i]=new StateTransferApplication(semaphore, names[i], from, to);
                else
                    apps[i]=new StateTransferApplication((JChannel)apps[0].getChannel(), semaphore, names[i], from, to);
                from+=MSG_SEND_COUNT;
                to+=MSG_SEND_COUNT;
            }

            for(int i=0;i < apps.length;i++) {
                StateTransferApplication app=apps[i];
                app.start();
                semaphore.release();
                Util.sleep(3000); // to reduce changes of a merge
            }

            // Make sure everyone is in sync
            Channel[] tmp=new Channel[apps.length];
            for(int i=0; i < apps.length; i++)
                tmp[i]=apps[i].getChannel();

            Util.waitUntilAllChannelsHaveSameSize(20000, 1000, tmp);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(apps.length, 20, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            // Sleep to ensure async messages arrive
            System.out.println("Waiting for all channels to have received the " + MSG_SEND_COUNT * APP_COUNT + " messages:");
            long end_time=System.currentTimeMillis() + 30000L;
            while(System.currentTimeMillis() < end_time) {
                boolean terminate=true;
                for(StateTransferApplication app: apps) {
                    Map map=app.getMap();
                    if(map.size() != MSG_SEND_COUNT * APP_COUNT) {
                        terminate=false;
                        break;
                    }
                }
                if(terminate)
                    break;
                else
                    Util.sleep(500);
            }

            // have we received all and the correct messages?
            System.out.println("++++++++++++++++++++++++++++++++++++++");
            for(int i=0;i < apps.length;i++) {
                StateTransferApplication w=apps[i];
                Map m=w.getMap();
                System.out.println("map has " + m.size() + " elements");
                assert m.size() == MSG_SEND_COUNT * APP_COUNT;
            }
            System.out.println("++++++++++++++++++++++++++++++++++++++");

            Set keys=apps[0].getMap().keySet();
            for(int i=0;i < apps.length;i++) {
                StateTransferApplication app=apps[i];
                Map m=app.getMap();
                Set s=m.keySet();
                assert keys.equals(s);
            }
        }
        finally {
            for(StateTransferApplication app: apps)
                app.getChannel().setReceiver(null);
            for(StateTransferApplication app: apps)
                app.cleanup();
        }
    }


    protected class StateTransferApplication extends PushChannelApplicationWithSemaphore {
        private final Map<Object,Object> map=new HashMap<Object,Object>(MSG_SEND_COUNT * APP_COUNT);
        private int from, to;

        public StateTransferApplication(Semaphore semaphore, String name, int from, int to) throws Exception {
            super(name, semaphore);
            this.from=from;
            this.to=to;
        }
        
        public StateTransferApplication(JChannel copySource,Semaphore semaphore, String name, int from, int to) throws Exception {
            super(copySource,name, semaphore);
            this.from=from;
            this.to=to;
        }

        public Map<Object,Object> getMap() {
            synchronized(map) {
                return Collections.unmodifiableMap(map);
            }
        }

        public void receive(Message msg) {
            Object[] data=(Object[])msg.getObject();
            int num_received=0;
            boolean changed=false;
            synchronized(map) {
                int tmp_size=map.size();
                map.put(data[0], data[1]);
                num_received=map.size();
                changed=tmp_size != num_received;
            }

            if(changed && num_received % 1000 == 0)
                System.out.println(channel.getAddress() + ": received " + num_received);

            // are we done?
            if(num_received >= MSG_SEND_COUNT * APP_COUNT)
                semaphore.release();
        }



        public void getState(OutputStream ostream) throws Exception {
            synchronized(map) {
                Util.objectToStream(map, new DataOutputStream(ostream));
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) throws Exception {
            Map<Object,Object> tmp=(Map<Object,Object>)Util.objectFromStream(new DataInputStream(istream));
            synchronized(map) {
                map.clear();
                map.putAll(tmp);
                System.out.println(channel.getAddress() + ": received state, map has " + map.size() + " elements");
            }
        }

        public void run() {
            boolean acquired=false;
            try {
                acquired=semaphore.tryAcquire(60000L, TimeUnit.MILLISECONDS);
                if(!acquired) {
                    throw new Exception(channel.getAddress() + " cannot acquire semaphore");
                }
                useChannel();
            }
            catch(Exception e) {
                log.error(channel.getAddress() + ": " + e.getLocalizedMessage(), e);
                exception=e;
            }
        }

        protected void useChannel() throws Exception {
            System.out.println(channel.getName() + ": connecting and fetching the state");
            channel.connect("StateTransferTest", null, 30000);
            System.out.println(channel.getName() + ": state transfer is done");
            Object[] data=new Object[2];
            for(int i=from; i < to; i++) {
                data[0]=new Integer(i);
                data[1]="Value #" + i;
                try {
                    channel.send(null, data);
                    if(i % 100 == 0)
                        Util.sleep(50);

                    if(i % 1000 == 0)
                        System.out.println(channel.getAddress() + ": sent " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


}