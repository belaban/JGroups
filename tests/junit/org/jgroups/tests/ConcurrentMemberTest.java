package org.jgroups.tests;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Test case modelling http://jira.jboss.com/jira/browse/JGRP-659
 * @author unknown
 * @version $Id: ConcurrentMemberTest.java,v 1.1 2008/04/25 06:59:17 belaban Exp $
 */
@Test(groups="temp", sequential=true)
public class ConcurrentMemberTest extends ChannelTestBase {
    private GroupManager coordinator=null;
    private List<GroupManager> managers=null;

    @BeforeMethod
    void beforeEachTest() {
        coordinator=startCoordinator();
        managers=new ArrayList<GroupManager>();
    }

    @AfterMethod
    void afterEachTest() throws Exception {
        for(GroupManager manager : managers) {
            manager.shutdown();
            manager.join(20000); // NOTE: Have seen shutdown take roughly 15 seconds, so wait 20

            if(manager.isAlive()) {
                log.info("Attempted to join manager " + manager.getName() + ", but it is still running.  Failing Test.");
                throw new IllegalStateException("Unable to properly shutdown jgroups connection.");
            }
            else {
                log.info("Joined - " + manager.getName());
                // Give others time to deal with leaving
                Util.sleep(1000);
            }
        }

        coordinator.shutdown();
        coordinator.join();
    }

    public void testStartingTwoConcurrentConnections() {
        doConnections(2);
    }

    public void testStartingFiveConcurrentConnections() {
        doConnections(5);
    }

    public void testStartingFifteenConcurrentConnections() {
        doConnections(15);
    }

    private GroupManager startCoordinator() {
        coordinator=new GroupManager("cluster1", "coordinator");
        coordinator.start();

        // Wait for coordinator to stabilize
        Util.sleep(3000);
        return coordinator;
    }

    private void doConnections(int count) {
        for(int i=0; i < count; i++) {
            managers.add(new GroupManager("cluster1", "connection-" + i));
        }

        for(GroupManager manager : managers) {
            manager.start();
        }

        waitForMemberCount(managers, count + 1);
        waitForState(managers);

        // Make sure they are all the same size
        for(GroupManager manager : managers) {
            assert manager.group.getCachedMembers().size() == managers.size() + 1 : "incorrect number of members";
            log.info(manager.group.getId() + " sees " + manager.group.getCachedMembers());
        }

        // Make sure they all have the same state
        String state=null;
        for(GroupManager manager : managers) {
            if(state == null) {
                // Set the state initially, and make sure it is not null
                state=manager.group.getState();
                assert state != null;
            }
            else {
                // Compare this manager's state to the first
                assert manager.group.getState().equals(state);
            }
        }
    }

    private void waitForMemberCount(List<GroupManager> managers, int count) {
        long start=System.currentTimeMillis();
        long end=start + (10 * 1000 * managers.size());
        int numReady=0;

        while(numReady < managers.size() && System.currentTimeMillis() < end) {
            numReady=0;

            for(GroupManager manager : managers) {
                if(manager.group.getCachedMembers() != null &&
                        manager.group.getCachedMembers().size() >= (managers.size() + 1)) {
                    log.info("Channel has expected member count - "  + manager.getName());
                    numReady++;
                }
                else {
                    int memberCount=0;

                    if(manager.group.getCachedMembers() != null) {
                        memberCount=manager.group.getCachedMembers().size();
                    }

                    log.info(manager.getName() + " needs more peers ["
                            + memberCount + " of " + (managers.size() + 1)
                            + "]" + "[coord=" + manager.group.getCoordinator()
                            + "]");
                }
            }

            if(numReady < managers.size()) {
                Util.sleep(1000);
            }
        }

        assert managers.size() == numReady : "not all members see each other";
    }

    private void waitForState(List<GroupManager> managers) {
        long start=System.currentTimeMillis();
        long end=start + (10 * 1000 * managers.size());
        int numReady=0;

        while(numReady < managers.size() && System.currentTimeMillis() < end) {
            numReady=0;

            for(GroupManager manager : managers) {
                if(manager.group.getState() != null) {
                    log.info(manager.getName() + " has state - "
                            + manager.group.getState());
                    numReady++;
                }
                else {
                    log.info(manager.getName() + " needs state");
                }
            }

            if(numReady < managers.size()) {
                Util.sleep(500);
            }
        }

        assert managers.size() == numReady : "not all managers have received state";
    }

    private class GroupManager extends Thread {
        GroupConnection group=null;

        volatile boolean shutdown=false;

        public GroupManager(String clusterName, String name) {
            group=new GroupConnection(clusterName);
            this.setName(name);
        }

        public void shutdown() {
            shutdown=true;

            group.disconnect();
        }

        public void run() {
            group.connect();

            setName(getName() + "[" + group.getId() + "]");

            while(!shutdown) {
                Util.sleep(1000);
            }
        }
    }



    private interface Channel {
        void connect();

        Object receive(long timeout);

        void publish(Message message);

        void disconnect();

        boolean requestState(Address member, long timeout);

        void returnState(byte[] state);

        String getId();

        Address getSelf();

        List<Address> getMembers();
    }

    private class MulticastChannel implements Channel {
        private static final String jgroupsConfig="udp.xml";

        private String clusterName;

        private JChannel jChannel;

        public MulticastChannel(String clusterName) {
            this.clusterName=clusterName;

            try {
                jChannel=new JChannel(jgroupsConfig);
            }
            catch(ChannelException ce) {
                throw new RuntimeException(ce);
            }
        }

        public void connect() {
            try {
                jChannel.connect(clusterName);
            }
            catch(ChannelException ce) {
                throw new RuntimeException(ce);
            }
        }

        public Object receive(long timeout) {
            try {
                Object obj=jChannel.receive(timeout);

                if(obj != null) {
                    log.info(getId() + "received message from JGroups -" + obj);
                    return obj;
                }
            }
            catch(ChannelException ce) {
                throw new RuntimeException(ce);
            }
            catch(TimeoutException te) {
                // Will happen
            }

            return null;
        }

        public boolean isConnected() {
            return jChannel.isConnected();
        }

        public void disconnect() {
            jChannel.close();
        }

        public void publish(Message message) {
            if(!jChannel.isConnected()) {
                throw new IllegalStateException(
                        "Unable to publish message when closed.");
            }

            try {
                jChannel.send(new org.jgroups.Message());
            }
            catch(ChannelException ce) {
                throw new RuntimeException(ce);
            }
        }

        public boolean requestState(Address member, long timeout) {
            try {
                return jChannel.getState(member, timeout);
            }
            catch(ChannelException ce) {
                throw new RuntimeException(ce);
            }
        }

        public void returnState(byte[] state) {
            jChannel.returnState(state);
        }

        public String getId() {
            return jChannel.getLocalAddress() == null? null : jChannel
                    .getLocalAddress().toString();
        }

        public Address getSelf() {
            return jChannel.getLocalAddress();
        }

        public List<Address> getMembers() {
            List<Address> retVal=new ArrayList<Address>();

            if(isConnected()) {
                View view=jChannel.getView();
                Vector<Address> addresses=view.getMembers();

                for(Address address : addresses) {
                    retVal.add(address);
                }
            }

            return retVal;
        }
    }

    public class GroupConnection {
        protected final Log log=LogFactory.getLog(this.getClass());

        private Channel channel=null;

        private ReceiverThread receiver=null;

        private List<Address> members=null;

        private ReentrantLock stateLock=null;

        private volatile boolean isRequestingState=false;

        private String state=null;

        public GroupConnection() {
            this("default-group");
        }

        public GroupConnection(String name) {
            channel=new MulticastChannel(name);

            stateLock=new ReentrantLock();
        }

        public String getId() {
            return channel.getId();
        }

        public void connect() {
            channel.connect();

            receiver=new ReceiverThread(channel);
            receiver.start();
        }

        public void disconnect() {
            if(receiver == null) {
                return;
            }

            receiver.shutdown();

            try {
                receiver.join();
            }
            catch(InterruptedException ie) {
                // Keep going
            }

            channel.disconnect();
        }

        public String getState() {
            return state;
        }

        /**
         * The cached members are set when handling view messages. This will be
         * used to test if the view changes are in synch with the channel.
         * @return
         */
        public List<Address> getCachedMembers() {
            return members;
        }

        public Address getCoordinator() {
            List<Address> members=channel.getMembers();

            if(members != null && members.size() > 0) {
                return members.get(0);
            }
            else {
                return null;
            }
        }

        /**
         * Get the members directly from the contained channel. This will be
         * used to test if the view changes are in synch with the channel.
         * @return
         */
        public List<Address> getChannelMembers() {
            return channel.getMembers();
        }

        private class ReceiverThread extends Thread {
            private Channel channel=null;

            private volatile boolean shutdown=false;

            public ReceiverThread(Channel channel) {
                this.channel=channel;
            }

            public void shutdown() {
                shutdown=true;
            }

            public void run() {
                while(!shutdown) {
                    Object o=channel.receive(5000);

                    if(o != null) {
                        if(o instanceof Message) {
                            log.info(channel.getId()
                                    + "received data message -"
                                    + new String(((Message)o).getBuffer()));
                        }
                        else if(o instanceof View) {
                            log.info(channel.getId()
                                    + "recieved view message -" + o);

                            View view=(View)o;

                            members=view.getMembers();
                            log.info(channel.getId() + "cached members are "
                                    + members);

                            configureState(view);
                        }
                        else if(o instanceof GetStateEvent) {
                            log.info(channel.getId()
                                    + "recieved getstate message -" + o);

                            stateLock.lock();
                            try {
                                if(state == null) {
                                    channel
                                            .returnState("NO STATE CURRENTLY SET"
                                                    .getBytes());
                                }
                                else {
                                    channel.returnState(state.getBytes());
                                }
                            }
                            finally {
                                stateLock.unlock();
                            }
                        }
                        else if(o instanceof SetStateEvent) {
                            log.info(channel.getId()
                                    + "recieved setstate message -" + o);

                            byte[] bytes=(byte[])((SetStateEvent)o)
                                    .getArg();

                            try {
                                stateLock.lock();
                                state=new String(bytes);

                                log.info(channel.getId() + "set state to '"
                                        + state + "'");
                            }
                            finally {
                                stateLock.unlock();
                            }
                        }
                        else if(o instanceof ExitEvent) {
                            log.info(channel.getId()
                                    + "recieved exit message -" + o);

                            disconnect();
                            connect();
                        }
                    }
                    else {
                        log.info(channel.getId()
                                + "has no messages after 5 seconds.");
                    }
                }
            }

            private void configureState(View view) {
                if(view instanceof MergeView) {
                    MergeView mergeView=(MergeView)view;

                    if(!isMemberOfLargestSubgroup(mergeView, channel.getSelf())) {
                        log
                                .info(channel.getId()
                                        + "is not member of largest subgroup, need to get state");

                        // Not member of largest group, so get state from
                        // coordinator of largest group

                        Address coordinatorToAskForState=findCoordinatorOfLargestSubgroup(mergeView);

                        requestStateInSeparateThread(coordinatorToAskForState);
                    }
                }
                else {
                    // If I am the coordinator, then set the initial state
                    if(view.getVid().getCoordAddress().toString().equals(
                            channel.getId())) {

                        if(state == null) {
                            stateLock.lock();
                            try {
                                state="STATE: startup time="
                                        + System.currentTimeMillis();

                                log.info("INITIALIZING STATE: " + state);
                            }
                            finally {
                                stateLock.unlock();
                            }
                        }
                    }
                    else {
                        stateLock.lock();

                        try {
                            // Setting the view, and don't have state yet, so
                            // retrieve it
                            if(state == null) {
                                requestStateInSeparateThread(view.getVid()
                                        .getCoordAddress());
                            }
                        }
                        finally {
                            stateLock.unlock();
                        }
                    }
                }
            }

            private void requestStateInSeparateThread(final Address stateHolder) {
                Runnable r=new Runnable() {
                    public void run() {
                        requestState(stateHolder);
                    }
                };

                Thread t=new Thread(r);
                t.start();
            }

            private boolean requestState(Address stateHolder) {
                boolean retVal=true;

                stateLock.lock();
                try {
                    if(!isRequestingState) {
                        isRequestingState=true;
                        state=null;

                        int count=0;

                        log.info(channel.getId() + "getting state from"
                                + stateHolder);

                        while(!channel.requestState(stateHolder, 10000)) {
                            log
                                    .info(channel.getId()
                                            + "failed getting state from"
                                            + stateHolder);

                            if(++count > 6) {
                                isRequestingState=false;

                                log
                                        .info(channel.getId()
                                                + "Failed to get state after 6 attempts, exiting.");
                                retVal=false;
                                break;
                            }
                        }

                        if(retVal) {
                            log.info(channel.getId()
                                    + "successfully requested state from"
                                    + stateHolder);
                        }
                    }
                }
                finally {
                    isRequestingState=false;
                    stateLock.unlock();
                }

                return retVal;
            }

            private boolean isMemberOfLargestSubgroup(MergeView mergeView, Address member) {
                View largestSubgroup=getLargestSubgroup(mergeView);
                return largestSubgroup != null && largestSubgroup.containsMember(member);
            }

            private Address findCoordinatorOfLargestSubgroup(MergeView mergeView) {
                View largestSubgroup=getLargestSubgroup(mergeView);

                if(largestSubgroup != null) {
                    return largestSubgroup.getVid().getCoordAddress();
                }

                return null;
            }

            private View getLargestSubgroup(MergeView mergeView) {
                View largestSubgroup=null;

                for(View subgroup : mergeView.getSubgroups()) {
                    if(largestSubgroup == null
                            || subgroup.size() > largestSubgroup.size()) {
                        largestSubgroup=subgroup;
                    }
                }

				return largestSubgroup;
			}
		}
	}
}
