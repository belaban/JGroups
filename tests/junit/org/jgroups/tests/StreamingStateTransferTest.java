package org.jgroups.tests;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

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
        transferHelper(channelNames, false);
    }

    @Test
    public void testRpcChannelTransfer() {
        String channelNames[]= { "A", "B", "C", "D" };
        transferHelper(channelNames, true);
    }

    private void transferHelper(String channelNames[], boolean useDispatcher) {
        transferHelper(channelNames, false, false, useDispatcher);
    }

    private void transferHelper(String channelNames[],
                                boolean crash,
                                boolean largeTransfer,
                                boolean useDispatcher) {
        int channelCount=channelNames.length;
        ArrayList<StreamingStateTransferApplication> channels=new ArrayList<StreamingStateTransferApplication>(channelCount);

        // Create a semaphore and take all its tickets
        Semaphore semaphore=new Semaphore(channelCount);

        try {

            semaphore.acquire(channelCount);
            boolean crashed=false;
            // Create activation threads that will block on the semaphore
            for(int i=0;i < channelCount;i++) {
                StreamingStateTransferApplication channel=null;
                if(i == 0)
                    channel=new StreamingStateTransferApplication(channelNames[i],
                                                                  semaphore,
                                                                  useDispatcher,
                                                                  largeTransfer);
                else
                    channel=new StreamingStateTransferApplication((JChannel)channels.get(0)
                                                                                    .getChannel(),
                                                                  channelNames[i],
                                                                  semaphore,
                                                                  useDispatcher,
                                                                  largeTransfer);

                // Start threads and let them join the channel
                channels.add(channel);
                semaphore.release(1);
                channel.start();
                Util.sleep(2000);

                if(crash && !crashed && i > 2) {
                    StreamingStateTransferApplication coord=channels.remove(0);
                    coord.cleanup();
                    crashed=true;
                }
            }

            blockUntilViewsReceived(channels, 60000);

            // Reacquire the semaphore tickets; when we have them all
            // we know the threads are done
            boolean acquired=semaphore.tryAcquire(channelCount, 60, TimeUnit.SECONDS);
            if(!acquired) {
                log.warn("Most likely a bug, analyse the stack below:");
                log.warn(Util.dumpThreads());
            }

            int getStateInvokedCount=0;
            int setStateInvokedCount=0;
            int partialGetStateInvokedCount=0;
            int partialSetStateInvokedCount=0;

            Util.sleep(3000);
            for(int i=0;i < channels.size();i++) {
                StreamingStateTransferApplication current=channels.get(i);
                if(current.getStateInvoked) {
                    getStateInvokedCount++;
                }
                if(current.setStateInvoked) {
                    setStateInvokedCount++;
                }
                if(current.partialGetStateInvoked) {
                    partialGetStateInvokedCount++;
                }
                if(current.partialSetStateInvoked) {
                    partialSetStateInvokedCount++;
                }
                Map<Address,List<Integer>> map=current.getMap();
                for(int j=0;j < channels.size();j++) {
                    StreamingStateTransferApplication app=channels.get(j);
                    List<?> l=map.get(app.getLocalAddress());
                    int size=l != null? l.size() : 0;
                    Assert.assertEquals(size,
                                        StreamingStateTransferApplication.COUNT,
                                        "Correct element count in map ");
                }
            }

            Assert.assertEquals(getStateInvokedCount, 1, "Correct invocation count of getState ");
            Assert.assertEquals(setStateInvokedCount,
                                channelCount - 1,
                                "Correct invocation count of setState ");
            Assert.assertEquals(partialGetStateInvokedCount,
                                1,
                                "Correct invocation count of partial getState ");
            Assert.assertEquals(partialSetStateInvokedCount,
                                channelCount - 1,
                                "Correct invocation count of partial setState ");

        }
        catch(Exception ex) {
            log.warn(ex);
        }
        finally {
            for(int i=0;i < channels.size();i++) {
                StreamingStateTransferApplication app=channels.get(i);
                Util.sleep(500);
                app.cleanup();
            }
        }
    }

    protected class StreamingStateTransferApplication extends PushChannelApplicationWithSemaphore {
        private final Map<Address,List<Integer>> stateMap=new HashMap<Address,List<Integer>>();

        public static final int COUNT=25;

        boolean partialSetStateInvoked=false;

        boolean partialGetStateInvoked=false;

        boolean setStateInvoked=false;

        boolean getStateInvoked=false;

        boolean largeTransfer=false;

        public StreamingStateTransferApplication(String name,
                                                 Semaphore s,
                                                 boolean useDispatcher,
                                                 boolean largeTransfer) throws Exception {
            super(name, s, useDispatcher);
            this.largeTransfer=largeTransfer;
            channel.connect("StreamingStateTransferApplication");
        }

        public StreamingStateTransferApplication(JChannel ch,
                                                 String name,
                                                 Semaphore s,
                                                 boolean useDispatcher,
                                                 boolean largeTransfer) throws Exception {
            super(ch, name, s, useDispatcher);
            this.largeTransfer=largeTransfer;
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
            channel.getState(null, name, 25000);
        }

        public void getState(OutputStream ostream) {
            if(largeTransfer)
                Util.sleep(4000);

            super.getState(ostream);
            ObjectOutputStream oos=null;
            try {
                oos=new ObjectOutputStream(ostream);
                HashMap copy=null;
                synchronized(stateMap) {
                    copy=new HashMap(stateMap);
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
            if(largeTransfer)
                Util.sleep(4000);

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

        public void setState(byte[] state) {
            if(largeTransfer)
                Util.sleep(4000);

            Map result=null;
            try {
                result=(Map)Util.objectFromByteBuffer(state);
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

        public void setState(InputStream istream) {
            if(largeTransfer)
                Util.sleep(4000);

            super.setState(istream);
            ObjectInputStream ois=null;
            try {
                ois=new ObjectInputStream(istream);
                Map map=(Map)ois.readObject();
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

        public void setState(String stateId, byte[] state) {
            if(largeTransfer)
                Util.sleep(4000);

            Object nameTransfer=null;
            try {
                nameTransfer=Util.objectFromByteBuffer(state);
                assertEquals("Got partial state requested ", nameTransfer, name);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                partialSetStateInvoked=true;
            }
        }

        public byte[] getState(String stateId) {
            if(largeTransfer)
                Util.sleep(4000);

            byte[] result=null;
            try {
                result=Util.objectToByteBuffer(stateId);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                partialGetStateInvoked=true;
            }
            return result;
        }

        public void setState(String state_id, InputStream istream) {
            if(largeTransfer)
                Util.sleep(4000);

            super.setState(state_id, istream);
            ObjectInputStream ois=null;
            try {
                ois=new ObjectInputStream(istream);
                assertEquals("Got partial state requested ", ois.readObject(), name);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                partialSetStateInvoked=true;
                Util.close(ois);
            }
        }

        public void getState(String state_id, OutputStream ostream) {
            if(largeTransfer)
                Util.sleep(4000);

            super.getState(state_id, ostream);
            ObjectOutputStream oos=null;
            try {
                oos=new ObjectOutputStream(ostream);
                oos.writeObject(state_id);
                oos.flush();
            }
            catch(IOException e) {
                e.printStackTrace();
            }
            finally {
                partialGetStateInvoked=true;
                Util.close(oos);
            }
        }
    }
}
