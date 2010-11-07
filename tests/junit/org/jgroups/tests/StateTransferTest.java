package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
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
 * @version $Id: StateTransferTest.java,v 1.35 2009/10/01 21:09:55 vlada Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=false)
public class StateTransferTest extends ChannelTestBase {
    static final int MSG_SEND_COUNT=5000;
    static final String[] names= { "A", "B", "C", "D"};
    static final int APP_COUNT=names.length;

    @Test
    public void testStateTransferFromSelfWithRegularChannel() throws Exception {
        Channel ch=createChannel(true);
        ch.connect("StateTransferTest");
        try {
            boolean rc=ch.getState(null, 2000);
            assert !rc : "getState() on singleton should return false";
        }
        finally {
            ch.close();
        }
    }

    @Test
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
                //avoid merge
                Util.sleep(3000);
            }

            // Make sure everyone is in sync
            Channel[] tmp=new Channel[apps.length];
            for(int i=0; i < apps.length; i++)
                tmp[i]=apps[i].getChannel();

            Util.blockUntilViewsReceived(60000, 1000, tmp);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(apps.length, 30, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            // Sleep to ensure async messages arrive
            System.out.println("Waiting for all channels to have received the " + MSG_SEND_COUNT * APP_COUNT + " messages:");
            long end_time=System.currentTimeMillis() + 40000L;
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
                log.info("map has " + m.size() + " elements");
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
                log.info(channel.getAddress() + ": received " + num_received);

            // are we done?
            if(num_received >= MSG_SEND_COUNT * APP_COUNT)
                semaphore.release();
        }

        public byte[] getState() {
            synchronized(map) {
                try {
                    return Util.objectToByteBuffer(map);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        public void setState(byte[] state) {
            synchronized(map) {
                try {
                    Map<Object,Object> tmp=(Map<Object,Object>)Util.objectFromByteBuffer(state);
                    map.putAll(tmp);
                    log.info(channel.getAddress() + ": received state, map has " + map.size() + " elements");
                }
                catch(Exception e) {
                    e.printStackTrace();
                }

            }
        }

        public void getState(OutputStream ostream) {
            synchronized(map) {
                try {
                    ObjectOutputStream out=new ObjectOutputStream(ostream);
                    out.writeObject(map);
                    out.close();
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) {
            synchronized(map) {
                try {
                    ObjectInputStream in=new ObjectInputStream(istream);
                    Map<Object,Object> tmp=(Map<Object,Object>)in.readObject();
                    Util.close(in);
                    map.putAll(tmp);
                    log.info(channel.getAddress() + ": received state, map has " + map.size() + " elements");
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
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
            channel.connect("StateTransferTest", null, null, 30000);
            System.out.println(channel.getName() + ": state transfer is done");
            Object[] data=new Object[2];
            for(int i=from; i < to; i++) {
                data[0]=new Integer(i);
                data[1]="Value #" + i;
                try {
                    channel.send(null, null, data);
                    if(i % 100 == 0)
                        Util.sleep(50);

                    if(i % 1000 == 0)
                        log.info(channel.getAddress() + ": sent " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


}