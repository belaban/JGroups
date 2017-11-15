package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.tom.TOA;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Some tests for TOA protocol.
 *
 * @author Pedro Ruivo
 * @since 3.5
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class TOATest {

    private final List<TOANode> registeredToaNodes = new ArrayList<>(4);

    @AfterMethod(alwaysRun = true)
    public void closeAndDisconnect() {
        registeredToaNodes.forEach(TOANode::stop);
        registeredToaNodes.clear();
    }

    public void testSingleDestinationSingleMember() throws Exception {
        doSingleDestinationTest(false);
    }

    public void testTotalOrderSingleDestinationSingleMember() throws Exception {
        doSingleDestinationTest(true);
    }

    public void testSingleDestinationWithMultipleMembers() throws Exception {
        doSingleDestinationWithMultipleMembersTest(false, 4);
    }

    public void testTotalOrderSingleDestinationWithMultipleMembers() throws Exception {
        doSingleDestinationWithMultipleMembersTest(true, 4);
    }

    public void testTotalOrder2() throws Exception {
        doTotalOrderTest(2);
    }

    public void testTotalOrder3() throws Exception {
        doTotalOrderTest(3);
    }

    public void testTotalOrder4() throws Exception {
        doTotalOrderTest(4);
    }

    public void testTotalOrder5() throws Exception {
        doTotalOrderTest(5);
    }

    public void testTotalOrderWithSingleDestination2() throws Exception {
        doTotalOrderTestWithSingleDestinationMessages(2);
    }

    public void testTotalOrderWithSingleDestination3() throws Exception {
        doTotalOrderTestWithSingleDestinationMessages(3);
    }

    public void testTotalOrderWithSingleDestination4() throws Exception {
        doTotalOrderTestWithSingleDestinationMessages(4);
    }

    public void testTotalOrderWithSingleDestination5() throws Exception {
        doTotalOrderTestWithSingleDestinationMessages(5);
    }

    private void doTotalOrderTest(int numberOfMembers) throws Exception {
        final int maxMessages = 1024;
        final int totalMessages = maxMessages * numberOfMembers;
        for (int i = 0; i < numberOfMembers; ++i) {
            TOANode node = createNewChannel("to_test_" + i);
            node.start("to_test_cluster_" + numberOfMembers);
            node.setEstimatedMessages(totalMessages);
        }
        for (TOANode member : registeredToaNodes) {
            member.waitAllMembers(registeredToaNodes, 30, TimeUnit.SECONDS);
        }
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfMembers);
        final CyclicBarrier barrier = new CyclicBarrier(numberOfMembers);
        final CountDownLatch shutdown = new CountDownLatch(numberOfMembers);
        for (TOANode node : registeredToaNodes) {
            executor.execute(new TOANodeRunnable(node) {
                @Override
                public void run() {
                    final String address = toaNode.channel.getAddressAsString();
                    try {
                        barrier.await();
                        for (int i = 0; i < maxMessages; ++i) {
                            toaNode.sendTotalOrderAnycastMessage(new AnycastAddress(), "to-message-" + address + "-" + i);
                        }
                        shutdown.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        shutdown.await();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        assertTrue(executor.shutdownNow().isEmpty());
        for (TOANode node : registeredToaNodes) {
            node.waitMessages(totalMessages, 30, TimeUnit.SECONDS);
        }
        for (int i = 0; i < totalMessages; ++i) {
            Iterator<TOANode> iterator = registeredToaNodes.iterator();
            String message = iterator.next().pop();
            while (iterator.hasNext()) {
                TOANode node = iterator.next();
                assertEquals("wrong message order #" + i + " for " + node.channel.getAddress(),
                        message, node.pop());
            }
        }
    }

    private void doTotalOrderTestWithSingleDestinationMessages(final int numberOfMembers) throws Exception {
        final int maxMessages = 1024;
        //one member will only send single destination messages
        final int totalMessages = maxMessages * (numberOfMembers -1);
        for (int i = 0; i < numberOfMembers; ++i) {
            TOANode node = createNewChannel("to_test_single_" + i);
            node.start("to_test_single_cluster_" + numberOfMembers);
            node.setEstimatedMessages(totalMessages);
        }
        for (TOANode member : registeredToaNodes) {
            member.waitAllMembers(registeredToaNodes, 30, TimeUnit.SECONDS);
        }
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfMembers);
        final CyclicBarrier barrier = new CyclicBarrier(numberOfMembers);
        final CountDownLatch shutdown = new CountDownLatch(numberOfMembers);
        final Map<Address, AtomicInteger> perAddressCounter = new ConcurrentHashMap<>();
        boolean first = true;
        for (TOANode node : registeredToaNodes) {
            if (first) {
                first = false;
                executor.execute(new TOANodeRunnable(node) {
                    @Override
                    public void run() {
                        final String address = toaNode.channel.getAddressAsString();
                        final List<Address> addressList = toaNode.currentView.getMembers();
                        try {
                            barrier.await();
                            for (int i = 0; i < maxMessages; ++i) {
                                Address destination = addressList.get(i % numberOfMembers);
                                AtomicInteger counter = perAddressCounter.get(destination);
                                if (counter != null) {
                                    counter.incrementAndGet();
                                } else {
                                    perAddressCounter.put(destination, new AtomicInteger(1));
                                }
                                toaNode.sendTotalOrderAnycastMessage(new AnycastAddress(destination),
                                        "single-message-" + address + "-" + i);
                            }
                            shutdown.countDown();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            } else {
                executor.execute(new TOANodeRunnable(node) {
                    @Override
                    public void run() {
                        final String address = toaNode.channel.getAddressAsString();
                        try {
                            barrier.await();
                            for (int i = 0; i < maxMessages; ++i) {
                                toaNode.sendTotalOrderAnycastMessage(new AnycastAddress(), "to-message-" + address + "-" + i);
                            }
                            shutdown.countDown();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
        shutdown.await();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        assertTrue(executor.shutdownNow().isEmpty());
        for (TOANode node : registeredToaNodes) {
            AtomicInteger singleMessages = perAddressCounter.get(node.localAddress());
            node.waitMessages(totalMessages + (singleMessages == null ? 0 : singleMessages.get()), 30, TimeUnit.SECONDS);
        }
        Map<Address, AtomicInteger> perAddressCounter2 = new ConcurrentHashMap<>();
        for (int i = 0; i < totalMessages; ++i) {
            Iterator<TOANode> iterator = registeredToaNodes.iterator();
            TOANode firstNode = iterator.next();
            AtomicInteger counter = perAddressCounter2.get(firstNode.localAddress());
            if (counter == null) {
                counter = new AtomicInteger(0);
                perAddressCounter2.put(firstNode.localAddress(), counter);
            }
            String message = nextMessage(firstNode, counter, "to-message-");
            while (iterator.hasNext()) {
                TOANode node = iterator.next();
                counter = perAddressCounter2.get(node.localAddress());
                if (counter == null) {
                    counter = new AtomicInteger(0);
                    perAddressCounter2.put(node.localAddress(), counter);
                }
                String message2 = nextMessage(node, counter, "to-message-");
                assertEquals("wrong message order #" + i + " for " + node.channel.getAddress(),
                        message, message2);
            }
        }

        for (TOANode node : registeredToaNodes) {
            if (node.size() > 0) {
                AtomicInteger counter = perAddressCounter2.get(node.localAddress());
                if (counter == null) {
                    counter = new AtomicInteger(0);
                    perAddressCounter2.put(node.localAddress(), counter);
                }
                while (node.size() > 0) {
                    assertTrue(node.pop().startsWith("single-message-"));
                    counter.incrementAndGet();
                }

            }
        }

        assertEquals(perAddressCounter.size(), perAddressCounter2.size());
        for (Map.Entry<Address, AtomicInteger> entry : perAddressCounter.entrySet()) {
            final Address key = entry.getKey();
            final AtomicInteger value = entry.getValue();
            assertTrue("Error in single messages in node " + key, perAddressCounter2.containsKey(key));
            assertEquals("Error in number of single messages in node " + key, value.get(), perAddressCounter2.get(key).get());
        }
    }

    private void doSingleDestinationTest(boolean totalOrder) throws Exception {
        final int maxMessages = 1024;
        TOANode singleMember = createNewChannel("toa_single_member_" + totalOrder);
        singleMember.start("toa_single_member_cluster_" + totalOrder);
        singleMember.waitForMembersInView(30, TimeUnit.SECONDS, singleMember.localAddress());
        singleMember.setEstimatedMessages(maxMessages);

        for (int i = 0; i < maxMessages; ++i) {
            if (totalOrder) {
                singleMember.sendTotalOrderAnycastMessage(new AnycastAddress(), Integer.toString(i));
            } else {
                singleMember.sendAnycastMessage(new AnycastAddress(), Integer.toString(i));
            }
        }

        //wait until all is delivered
        singleMember.waitMessages(maxMessages, 30, TimeUnit.SECONDS);
        for (int i = 0; i < maxMessages; ++i)
            assertEquals("Wrong message.", Integer.toString(i), singleMember.pop());
    }

    private void doSingleDestinationWithMultipleMembersTest(boolean totalOrder, int members) throws Exception {
        final int maxMessages = 1024;
        for (int i = 0; i < members; ++i) {
            TOANode member = createNewChannel("toa_multiple_member_" + totalOrder + "_" + i);
            member.start("toa_multiple_member_cluster_" + totalOrder);
            member.setEstimatedMessages(maxMessages);
        }

        for (TOANode member : registeredToaNodes) {
            member.waitAllMembers(registeredToaNodes, 30, TimeUnit.SECONDS);
        }
        assertTrue("Expected more than one node. value=" + registeredToaNodes.size(), registeredToaNodes.size() > 1);
        final TOANode sender = registeredToaNodes.get(0);

        for (TOANode destination : registeredToaNodes) {
            for (int i = 0; i < maxMessages; ++i) {
                if (totalOrder) {
                    sender.sendTotalOrderAnycastMessage(new AnycastAddress(destination.localAddress()), Integer.toString(i));
                } else {
                    sender.sendAnycastMessage(new AnycastAddress(destination.localAddress()), Integer.toString(i));
                }
            }
        }

        for (TOANode destination : registeredToaNodes) {
            //wait until all is delivered
            destination.waitMessages(maxMessages, 30, TimeUnit.SECONDS);
            for (int i = 0; i < maxMessages; ++i)
                assertEquals("Wrong message in member " + destination.channel.getAddress(),
                             Integer.toString(i), destination.pop());
        }
    }

    private TOANode createNewChannel(String name) throws Exception {
        JChannel channel = new JChannel(new SHARED_LOOPBACK(),
                                        new SHARED_LOOPBACK_PING(),
                                        new MERGE3(),
                                        new NAKACK2(),
                                        new UNICAST3(),
                                        new TOA(),
                                        new GMS()).name(name);
        TOANode toaNode = new TOANode(channel);
        registeredToaNodes.add(toaNode);
        return toaNode;
    }

    private static class TOANodeRunnable implements Runnable {

        protected final TOANode toaNode;

        public TOANodeRunnable(TOANode toaNode) {
            this.toaNode = toaNode;
        }

        @Override
        public void run() {
        }
    }

    private static class TOANode extends ReceiverAdapter {

        private final Object viewLock = new Object();
        private final JChannel channel;
        private volatile View currentView;
        private final ArrayList<String> messages;

        private TOANode(JChannel channel) {
            if (channel == null) {
                throw new NullPointerException();
            }
            this.channel = channel;
            this.channel.setReceiver(this);
            this.messages = new ArrayList<>();
        }

        public void start(String clusterName) throws Exception {
            channel.connect(clusterName);
        }

        public void stop() {
            channel.disconnect();
            channel.close();
        }

        public void setEstimatedMessages(int estimatedMessages) {
            messages.ensureCapacity(estimatedMessages);
        }

        public Address localAddress() {
            return channel.getAddress();
        }

        public String pop() {
            synchronized (messages) {
                return messages.remove(0);
            }
        }

        public int size() {
            synchronized (messages) {
                return messages.size();
            }
        }

        public void waitForMembersInView(int timeout, TimeUnit timeUnit, Address... members) throws InterruptedException {
            int loops = 0;
            final int maxLoops = (int) ((timeUnit.toSeconds(timeout) / 2) + 1);
            final Collection<Address> memberCollection = Arrays.asList(members);
            synchronized (viewLock) {
                while (!currentView.getMembers().containsAll(memberCollection) && loops++ < maxLoops) {
                    viewLock.wait(TimeUnit.SECONDS.toMillis(2));
                }
            }
            assertTrue("Members is missing: view=" + currentView + ", expected members=" + memberCollection,
                    currentView.getMembers().containsAll(memberCollection));
        }

        public void waitAllMembers(Collection<TOANode> members, int timeout, TimeUnit timeUnit) throws InterruptedException {
            List<Address> addresses=members.stream().map(TOANode::localAddress).collect(Collectors.toList());
            waitForMembersInView(timeout, timeUnit, addresses.toArray(new Address[addresses.size()]));
        }

        public void waitMessages(int size, long timeout, TimeUnit timeUnit) throws InterruptedException {
            int loops = 0;
            final int maxLoops = (int) (timeUnit.toSeconds(timeout) / 2) + 1;
            synchronized (messages) {
                while (messages.size() < size && loops++ < maxLoops) {
                    messages.wait(TimeUnit.SECONDS.toMillis(2));
                }
            }
            assertEquals("Wrong number of messages received.", size, messages.size());
        }

        public void sendAnycastMessage(AnycastAddress anycastAddress, String data) throws Exception {
            Message msg = new BytesMessage(anycastAddress).setSrc(channel.getAddress())
              .setFlag(Message.Flag.NO_TOTAL_ORDER).setObject(data);
            channel.send(msg);
        }

        public void sendTotalOrderAnycastMessage(AnycastAddress anycastAddress, String data) throws Exception {
            Message msg = new BytesMessage(anycastAddress).setSrc(channel.getAddress()).setObject(data);
            channel.send(msg);
        }

        @Override
        public void receive(Message msg) {
            synchronized (messages) {
                messages.add(String.valueOf((Object)msg.getObject()));
            }
        }

        @Override
        public void viewAccepted(View view) {
            synchronized (viewLock) {
                this.currentView = view;
            }
        }
    }

    private static String nextMessage(TOANode node, AtomicInteger missCounter, String prefix) {
        String message = node.pop();
        while (!message.startsWith(prefix)) {
            missCounter.incrementAndGet();
            message = node.pop();
        }
        return message;
    }

}
