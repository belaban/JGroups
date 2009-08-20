package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests streaming state transfer.
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id$
 * 
 */
@Test(groups= { Global.FLUSH }, sequential=false)
public class StreamingStateTransferTest extends ChannelTestBase {



    @Test
    public void testTransfer() {
        String channelNames[]= { "A", "B", "C", "D" };
        int channelCount=channelNames.length;
        ArrayList<StreamingStateTransferApplication> channels=new ArrayList<StreamingStateTransferApplication>(channelCount);

        // Create a semaphore and take all its tickets
        Semaphore semaphore=new Semaphore(channelCount);

        try {

            semaphore.acquire(channelCount);
            // Create activation threads that will block on the semaphore
            for(int i=0;i < channelCount;i++) {
                StreamingStateTransferApplication channel;
                if(i == 0)
                    channel=new StreamingStateTransferApplication(channelNames[i],
                                                                  semaphore);
                else
                    channel=new StreamingStateTransferApplication((JChannel)channels.get(0).getChannel(),
                                                                  channelNames[i],
                                                                  semaphore);
                // Start threads and let them join the channel
                channels.add(channel);
                semaphore.release(1);
                channel.start();
                if(i == 0)
                    Util.sleep(3000);
            }

            Channel[] tmp=new Channel[channels.size()];
            int cnt=0;
            for(StreamingStateTransferApplication app: channels) {
                tmp[cnt++]=app.getChannel();
            }
            Util.blockUntilViewsReceived(60000, 1000, tmp);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(channelCount, 60, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            int getStateInvokedCount=0;
            int setStateInvokedCount=0;

            for(int i=0;i < channels.size();i++) {
                StreamingStateTransferApplication current=channels.get(i);
                if(current.getStateInvoked) {
                    getStateInvokedCount++;
                }
                if(current.setStateInvoked) {
                    setStateInvokedCount++;
                }
                Map<Address,List<Integer>> map=current.getMap();
                for(int j=0;j < channels.size();j++) {
                    StreamingStateTransferApplication app=channels.get(j);
                    List<?> l=map.get(app.getLocalAddress());
                    int size=l != null? l.size() : 0;
                    assert size == StreamingStateTransferApplication.COUNT :
                            "expected " + StreamingStateTransferApplication.COUNT + " in map, but only found " + size +
                                    ", map=" + map;
                }
            }

            assert getStateInvokedCount == 1 : "expected get state invocation count of 1 but was " + getStateInvokedCount;
            assert setStateInvokedCount == channelCount - 1 :
                    "expected set state invocation count of " + (channelCount -1) + " but was " + setStateInvokedCount;
        }
        catch(Exception ex) {
            log.warn(ex.toString());
        }
        finally {
            for(StreamingStateTransferApplication app: channels)
                app.getChannel().setReceiver(null); // we don't want logging about view changes
            for(StreamingStateTransferApplication app: channels) {
                app.cleanup();
            }
        }
    }

    protected class StreamingStateTransferApplication extends PushChannelApplicationWithSemaphore {
        private final Map<Address,List<Integer>> stateMap=new HashMap<Address,List<Integer>>();

        public static final int COUNT=25;

        boolean setStateInvoked=false;
        boolean getStateInvoked=false;


        public StreamingStateTransferApplication(String name, Semaphore s) throws Exception {
            super(name, s);
            channel.connect("StreamingStateTransferApplication");
        }

        public StreamingStateTransferApplication(JChannel ch,
                                                 String name,
                                                 Semaphore s) throws Exception {
            super(ch, name, s, false);
            channel.connect("StreamingStateTransferApplication");
        }

        public void receive(Message msg) {
            Address sender=msg.getSrc();
            synchronized(stateMap) {
                List<Integer> list=stateMap.get(sender);
                if(list == null) {
                    list=new ArrayList<Integer>();
                    stateMap.put(sender, list);
                }
                list.add((Integer)msg.getObject());
            }
        }

        public Map<Address,List<Integer>> getMap() {
            return stateMap;
        }

        public void useChannel() throws Exception {
            for(int i=0;i < COUNT;i++) {
                channel.send(null, null, new Integer(i));
            }
            channel.getState(null, 25000);
        }

        public void getState(OutputStream ostream) {
            super.getState(ostream);
            ObjectOutputStream oos=null;
            try {
                oos=new ObjectOutputStream(ostream);
                HashMap<Address,List<Integer>> copy=null;
                synchronized(stateMap) {
                    copy=new HashMap<Address,List<Integer>>(stateMap);
                }
                oos.writeObject(copy);
                oos.flush();
            }
            catch(IOException e) {
                e.printStackTrace();
            }
            finally {
                getStateInvoked=true;
                Util.close(oos);
            }
        }

        public byte[] getState() {
            byte[] result=null;
            try {
                synchronized(stateMap) {
                    result=Util.objectToByteBuffer(stateMap);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                getStateInvoked=true;
            }
            return result;
        }

        @SuppressWarnings("unchecked")
        public void setState(byte[] state) {
            Map<Address,List<Integer>> result=null;
            try {
                result=(Map<Address,List<Integer>>)Util.objectFromByteBuffer(state);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                setStateInvoked=true;
            }
            synchronized(stateMap) {
                stateMap.clear();
                stateMap.putAll(result);
            }
        }

        @SuppressWarnings("unchecked")
        public void setState(InputStream istream) {
            super.setState(istream);
            ObjectInputStream ois=null;
            try {
                ois=new ObjectInputStream(istream);
                Map<Address,List<Integer>> map=(Map<Address,List<Integer>>)ois.readObject();
                synchronized(stateMap) {
                    stateMap.clear();
                    stateMap.putAll(map);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                setStateInvoked=true;
                Util.close(ois);
            }
        }
    }
}
